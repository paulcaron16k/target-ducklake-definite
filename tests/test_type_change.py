"""A property declared a string, whose column the table has as another type.

DUCKLKD-137. DuckLake can't change the column to VARCHAR, so text of the
column's type loads as before, and a batch with other text fails before it is
loaded or staged, naming the column, its type and the values.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, ClassVar
from unittest.mock import patch

import pytest

from target_ducklake.connector import DuckLakeConnector
from target_ducklake.sinks import ducklakeSink
from tests.test_mid_run_schema import DELETED, _records, _run, _schema

if TYPE_CHECKING:
    from collections.abc import Iterator

    from tests.catalogs import Lake

STRING = {"type": ["null", "string"]}
# The type a property had in an earlier run: its schema, a value, its column
# type and how the value reads back as text; then text of that type sent as a
# string, and how it reads back.
EARLIER: dict[str, tuple[dict, object, str, str, str, str]] = {
    "integer": ({"type": ["null", "integer"]}, 5, "BIGINT", "5", "42", "42"),
    "number": (
        {"type": ["null", "number"]},
        1.5,
        "DECIMAL(38,9)",
        "1.500000000",
        "2.25",
        "2.250000000",
    ),
    "boolean": (
        {"type": ["null", "boolean"]},
        True,
        "BOOLEAN",
        "true",
        "false",
        "false",
    ),
    "date": (
        {"type": ["null", "string"], "format": "date"},
        "2024-01-02",
        "DATE",
        "2024-01-02",
        "2024-02-03",
        "2024-02-03",
    ),
    "json": ({"type": ["null", "object"]}, {"a": 1}, "JSON", '{"a": 1}', "[2]", "[2]"),
}
LOAD_METHODS = ["append", "merge", "overwrite"]
# type_source typing v a date, for TestTypeSourceKeepingText.
DATE_SOURCE = {"type_source": {"v": "date"}}


def _properties(value: dict, key: dict | None = None) -> dict:
    return {"id": key or {"type": "integer"}, "v": value}


def _load_earlier(lake: Lake, kind: str = "integer") -> None:
    """A table an earlier run loaded with ``v`` of ``kind``: row 7."""
    schema, value = EARLIER[kind][:2]
    _run(lake, [_schema(_properties(schema)), *_records([{"id": 7, "v": value}])])


def _rows(lake: Lake) -> list[tuple]:
    return lake.query("SELECT id, v::VARCHAR FROM lake.s.orders ORDER BY id")


def _column_type(lake: Lake) -> list[tuple]:
    return lake.query(
        "SELECT data_type FROM information_schema.columns "
        "WHERE table_name = 'orders' AND column_name = 'v'"
    )


@pytest.fixture
def caught(caplog: pytest.LogCaptureFixture) -> Iterator[pytest.LogCaptureFixture]:
    """The target's warnings, caught on its own logger (the SDK replaces root's)."""
    logger = logging.getLogger("target-ducklake")
    logger.addHandler(caplog.handler)
    caplog.handler.setLevel(logging.WARNING)
    try:
        yield caplog
    finally:
        logger.removeHandler(caplog.handler)


def _setup_warnings(caught: pytest.LogCaptureFixture) -> list[str]:
    return [
        record.getMessage()
        for record in caught.records
        if "declares string properties whose columns" in record.getMessage()
    ]


class TestTextOfTheColumnsTypeLoads:
    """As before: the INSERT casts it. A warning at setup names the column."""

    @pytest.mark.parametrize("load_method", LOAD_METHODS)
    @pytest.mark.parametrize("kind", list(EARLIER))
    def test_in_a_later_run(
        self,
        lake: Lake,
        kind: str,
        load_method: str,
        caught: pytest.LogCaptureFixture,
    ):
        """Loaded into the column, whose type is unchanged; warned once."""
        _load_earlier(lake, kind)
        _, _, column_type, earlier, text, loaded = EARLIER[kind]
        _run(
            lake,
            [
                _schema(_properties(STRING)),
                *_records([{"id": 1, "v": text}, {"id": 2, "v": None}]),
            ],
            load_method=load_method,
            batch_size_rows=1,
        )
        kept = [] if load_method == "overwrite" else [(7, earlier)]
        assert _rows(lake) == [(1, loaded), (2, None), *kept]
        assert _column_type(lake) == [(column_type,)]
        warned = _setup_warnings(caught)
        assert len(warned) == 1
        assert f"'v' ({column_type})" in warned[0]
        assert "can't change a column's type to VARCHAR in place" in warned[0]
        assert "recreate the column as VARCHAR" in warned[0]

    def test_a_key_merges_with_the_row_it_casts_to(self, lake: Lake):
        """The staged key '7' is cast to BIGINT 7 and replaces that row."""
        _load_earlier(lake)
        _run(
            lake,
            [
                _schema(_properties(STRING, key=STRING)),
                *_records([{"id": "7", "v": "8"}, {"id": "1", "v": "2"}]),
            ],
            load_method="merge",
        )
        assert _rows(lake) == [(1, "2"), (7, "8")]

    def test_in_a_mid_run_schema(self, lake: Lake):
        """The rows on both sides of the change load."""
        _load_earlier(lake)
        _run(
            lake,
            [
                _schema(_properties(EARLIER["integer"][0])),
                *_records([{"id": 1, "v": 1}]),
                _schema(_properties(STRING)),
                *_records([{"id": 2, "v": "2"}]),
            ],
        )
        assert _rows(lake) == [(1, "1"), (2, "2"), (7, "5")]

    def test_no_warning_and_no_check_for_a_text_column(
        self, lake: Lake, caught: pytest.LogCaptureFixture
    ):
        """A stream whose string properties are VARCHAR columns pays nothing."""
        _run(lake, [_schema(_properties(STRING)), *_records([{"id": 7, "v": "x"}])])
        with patch.object(DuckLakeConnector, "text_cast_findings") as check:
            _run(lake, [_schema(_properties(STRING)), *_records([{"id": 1, "v": "y"}])])
        check.assert_not_called()
        assert _setup_warnings(caught) == []


class TestOtherTextFails:
    """At the first batch holding it, naming the column, its type and the values."""

    @pytest.mark.parametrize("load_method", LOAD_METHODS)
    @pytest.mark.parametrize("kind", list(EARLIER))
    def test_the_batch_is_refused_and_the_table_unchanged(
        self, lake: Lake, kind: str, load_method: str
    ):
        """Nothing of the run loads; the column keeps its type."""
        _load_earlier(lake, kind)
        column_type, earlier = EARLIER[kind][2:4]
        message = (
            f"stream 'orders', table orders: column 'v' is {column_type}, "
            "and 1 value(s) aren't: 'abc'."
        )
        with pytest.raises(ValueError, match="column 'v' is") as refused:
            _run(
                lake,
                [_schema(_properties(STRING)), *_records([{"id": 1, "v": "abc"}])],
                load_method=load_method,
            )
        assert message in str(refused.value)
        assert "The batch wasn't loaded." in str(refused.value)
        assert "recreate the column as VARCHAR" in str(refused.value)
        assert _rows(lake) == [(7, earlier)]
        assert _column_type(lake) == [(column_type,)]

    @pytest.mark.parametrize("load_method", LOAD_METHODS)
    def test_at_the_first_bad_batch(self, lake: Lake, load_method: str):
        """Batches before it load (or are staged); none after it is staged.

        An overwrite fails there, not at the end of the run, and replaces
        nothing: the table keeps the earlier run's rows.
        """
        _load_earlier(lake)
        staged = []
        write = ducklakeSink.write_temp_file

        def spy(sink: ducklakeSink, context: dict) -> str:
            staged.append([r["v"] for r in context["records"]])
            return write(sink, context)

        with (
            patch.object(ducklakeSink, "write_temp_file", spy),
            pytest.raises(ValueError, match=r"column 'v' is BIGINT, and 1 value"),
        ):
            _run(
                lake,
                [
                    _schema(_properties(STRING)),
                    *_records([{"id": i, "v": v} for i, v in enumerate("1x23")]),
                ],
                load_method=load_method,
                batch_size_rows=1,
            )
        assert staged == [["1"], ["x"]]
        loaded = [] if load_method == "overwrite" else [(0, "1")]
        assert _rows(lake) == [*loaded, (7, "5")]

    @pytest.mark.parametrize("load_method", LOAD_METHODS)
    def test_a_key_that_doesnt_cast(self, lake: Lake, load_method: str):
        """A merge couldn't match it: refused alike."""
        _load_earlier(lake)
        with pytest.raises(ValueError, match=r"column 'id' is BIGINT, and 1 value"):
            _run(
                lake,
                [
                    _schema(_properties(EARLIER["integer"][0], key=STRING)),
                    *_records([{"id": "seven", "v": 1}]),
                ],
                load_method=load_method,
            )
        assert _rows(lake) == [(7, "5")]

    @pytest.mark.parametrize("load_method", LOAD_METHODS)
    def test_in_a_mid_run_schema(self, lake: Lake, load_method: str):
        """At the batch after the change.

        The records before it load as an append or merge would; an overwrite
        replaces nothing.
        """
        _load_earlier(lake)
        with pytest.raises(ValueError, match=r"column 'v' is BIGINT"):
            _run(
                lake,
                [
                    _schema(_properties(EARLIER["integer"][0])),
                    *_records([{"id": 1, "v": 1}]),
                    _schema(_properties(STRING)),
                    *_records([{"id": 2, "v": "x"}]),
                ],
                load_method=load_method,
            )
        before_change = [] if load_method == "overwrite" else [(1, "1")]
        assert _rows(lake) == [*before_change, (7, "5")]

    def test_the_values_are_counted_sampled_and_cut(self, lake: Lake):
        """Distinct and sorted, three at most, each cut to 40 characters."""
        _load_earlier(lake)
        values = ["a" * 100, "b", "c", "d", "b", "9"]
        with pytest.raises(ValueError, match="column 'v' is BIGINT") as refused:
            _run(
                lake,
                [
                    _schema(_properties(STRING)),
                    *_records([{"id": i, "v": v} for i, v in enumerate(values)]),
                ],
            )
        assert f"5 value(s) aren't: '{'a' * 40}...', 'b', 'c'." in str(refused.value)

    def test_every_column_is_named(self, lake: Lake):
        """In one error."""
        properties = {
            "id": {"type": "integer"},
            "a": {"type": "integer"},
            "b": {"type": "boolean"},
        }
        _run(lake, [_schema(properties), *_records([{"id": 1, "a": 1, "b": True}])])
        strings = {"id": {"type": "integer"}, "a": STRING, "b": STRING}
        with pytest.raises(ValueError, match="column 'a' is BIGINT") as refused:
            _run(
                lake, [_schema(strings), *_records([{"id": 2, "a": "x", "b": "maybe"}])]
            )
        message = str(refused.value)
        assert "column 'b' is BOOLEAN, and 1 value(s) aren't: 'maybe'" in message


class TestRowsMarkedDeleted:
    """With delete_rows_with_deleted_at, a row marked deleted isn't inserted."""

    PROPERTIES: ClassVar[dict] = {
        **_properties(STRING),
        "_sdc_deleted_at": {"type": ["null", "string"], "format": "date-time"},
    }

    def _earlier(self, lake: Lake) -> None:
        _run(
            lake,
            [
                _schema(_properties(EARLIER["integer"][0])),
                *_records([{"id": 7, "v": 5}, {"id": 8, "v": 6}]),
            ],
        )

    @pytest.mark.parametrize("load_method", LOAD_METHODS)
    def test_its_other_columns_arent_checked(self, lake: Lake, load_method: str):
        """A tombstone's placeholder text loads nowhere, so it can't fail."""
        self._earlier(lake)
        _run(
            lake,
            [
                _schema(self.PROPERTIES),
                *_records(
                    [
                        {"id": 7, "v": "gone", "_sdc_deleted_at": DELETED},
                        {"id": 9, "v": "3"},
                    ]
                ),
            ],
            load_method=load_method,
            delete_rows_with_deleted_at=True,
        )
        kept = [] if load_method == "overwrite" else [(8, "6")]
        assert _rows(lake) == [*kept, (9, "3")]

    @pytest.mark.parametrize("load_method", LOAD_METHODS)
    def test_its_key_is_checked(self, lake: Lake, load_method: str):
        """Its key is cast to find the row it deletes."""
        self._earlier(lake)
        with pytest.raises(ValueError, match=r"column 'id' is BIGINT, and 1 value"):
            _run(
                lake,
                [
                    _schema({**self.PROPERTIES, "id": STRING}),
                    *_records([{"id": "seven", "v": "1", "_sdc_deleted_at": DELETED}]),
                ],
                load_method=load_method,
                delete_rows_with_deleted_at=True,
            )
        assert _rows(lake) == [(7, "5"), (8, "6")]


class TestTextChangedByTheCast:
    """Text that casts only by losing part of it follows value_type_checking."""

    # The earlier type, the text, and what it loads as.
    CHANGED: ClassVar[list[tuple[str, str, str]]] = [
        ("integer", "1.5", "2"),
        ("integer", "0x10", "16"),
        ("number", "1.0000000005", "1.000000001"),
        ("number", "1.0000000005e0", "1.000000001"),
        ("date", "2024-01-02T23:59:59+05:00", "2024-01-02"),
    ]

    @pytest.mark.parametrize(("kind", "text", "loaded"), CHANGED)
    def test_permissive_loads_it_with_a_warning(
        self,
        lake: Lake,
        kind: str,
        text: str,
        loaded: str,
        caught: pytest.LogCaptureFixture,
    ):
        """Counted and sampled, per batch."""
        _load_earlier(lake, kind)
        _run(lake, [_schema(_properties(STRING)), *_records([{"id": 1, "v": text}])])
        assert _rows(lake)[0] == (1, loaded)
        assert (
            f"column 'v' is {EARLIER[kind][2]}, and 1 value(s) lose part of their "
            f"text in its cast (a fraction it doesn't hold, text a number isn't "
            f"written as, or a time of day for a date): {text!r}. Loaded as cast"
        ) in caught.text

    @pytest.mark.parametrize("load_method", LOAD_METHODS)
    @pytest.mark.parametrize(("kind", "text", "loaded"), CHANGED)
    def test_strict_fails_the_batch(
        self, lake: Lake, kind: str, text: str, loaded: str, load_method: str
    ):
        """Before it is loaded or staged: the table keeps its rows."""
        _load_earlier(lake, kind)
        with pytest.raises(ValueError, match="value_type_checking is strict"):
            _run(
                lake,
                [_schema(_properties(STRING)), *_records([{"id": 1, "v": text}])],
                load_method=load_method,
                value_type_checking="strict",
            )
        assert _rows(lake) == [(7, EARLIER[kind][3])]
        assert loaded  # what permissive would have stored

    @pytest.mark.parametrize(
        ("kind", "text", "loaded"),
        [
            ("integer", "7.0", "7"),
            ("integer", "1e3", "1000"),
            ("integer", " 7 ", "7"),
            ("number", "1.123456789", "1.123456789"),
            ("date", "2024-01-02T00:00:00Z", "2024-01-02"),
        ],
    )
    def test_text_that_casts_exactly_passes_strict(
        self, lake: Lake, kind: str, text: str, loaded: str
    ):
        """Another spelling of the value isn't a change."""
        _load_earlier(lake, kind)
        _run(
            lake,
            [_schema(_properties(STRING)), *_records([{"id": 1, "v": text}])],
            value_type_checking="strict",
        )
        assert _rows(lake)[0][1].startswith(loaded)


class TestKeysThatCastToOneKey:
    """'7' and '7.0' in one batch load as two rows with key 7."""

    MESSAGES: ClassVar[list[dict]] = [
        _schema(_properties(STRING, key=STRING)),
        *_records([{"id": "7", "v": "1"}, {"id": "7.0", "v": "2"}]),
    ]
    # The loads that match rows by key.
    BY_KEY: ClassVar[list[str]] = ["merge", "overwrite"]

    @pytest.mark.parametrize("load_method", BY_KEY)
    def test_permissive_warns(
        self, lake: Lake, load_method: str, caught: pytest.LogCaptureFixture
    ):
        """The texts of the key are named."""
        _load_earlier(lake)
        _run(lake, self.MESSAGES, load_method=load_method)
        assert sorted(_rows(lake)) == [(7, "1"), (7, "2")]
        assert (
            "1 key text(s) cast to the key of another, so their records load as "
            "separate rows with one key: '7' = '7.0'"
        ) in caught.text

    @pytest.mark.parametrize("load_method", BY_KEY)
    def test_strict_fails_the_batch(self, lake: Lake, load_method: str):
        """The table keeps its rows."""
        _load_earlier(lake)
        with pytest.raises(ValueError, match=r"'7' = '7\.0'.*strict"):
            _run(
                lake,
                self.MESSAGES,
                load_method=load_method,
                value_type_checking="strict",
            )
        assert _rows(lake) == [(7, "5")]

    def test_append_keeps_every_row_anyway(
        self, lake: Lake, caught: pytest.LogCaptureFixture
    ):
        """It doesn't match rows by key, so nothing is checked, even under strict."""
        _load_earlier(lake)
        _run(lake, self.MESSAGES, load_method="append", value_type_checking="strict")
        assert sorted(_rows(lake)) == [(7, "1"), (7, "2"), (7, "5")]
        assert "key text(s)" not in caught.text

    def test_only_within_a_batch(self, lake: Lake, caught: pytest.LogCaptureFixture):
        """Pinned: across batches, an overwrite compares keys as staged text.

        With one record per batch, '7' and '7.0' are each alone in theirs,
        and the overwrite's last row per key (DUCKLKD-136) is by text.
        """
        _load_earlier(lake)
        _run(
            lake,
            self.MESSAGES,
            load_method="overwrite",
            batch_size_rows=1,
            value_type_checking="strict",
        )
        assert sorted(_rows(lake)) == [(7, "1"), (7, "2")]
        assert "key text(s)" not in caught.text


class TestColumnNames:
    """The check finds the column by the name the stream loads it as."""

    @pytest.mark.parametrize(
        ("prop", "column", "config"),
        [
            ("My Col", "my_col", {"conform_column_names": True}),
            # Truncated to 63 characters on a Postgres catalog.
            ("c" * 70, "c" * 70, {}),
        ],
    )
    def test_good_text_loads_and_bad_text_fails(
        self, lake: Lake, prop: str, column: str, config: dict
    ):
        """Named by the table's column."""
        if lake.kind == "postgres":
            column = column[:63]
        integer = {"id": {"type": "integer"}, prop: {"type": ["null", "integer"]}}
        _run(lake, [_schema(integer), *_records([{"id": 7, prop: 5}])], **config)
        strings = {"id": {"type": "integer"}, prop: STRING}
        _run(lake, [_schema(strings), *_records([{"id": 1, prop: "6"}])], **config)
        with pytest.raises(
            ValueError, match=rf"column '{column}' is BIGINT, and 1 value"
        ):
            _run(lake, [_schema(strings), *_records([{"id": 2, prop: "x"}])], **config)
        query = f'SELECT id, "{column}" FROM lake.s.orders ORDER BY id'  # noqa: S608
        assert lake.query(query) == [(1, 6), (7, 5)]


class TestTimestamptzColumn:
    """Text with or without an offset loads; other text fails."""

    def test_offsets_load_and_other_text_fails(self, lake: Lake):
        """As the instant the text names."""
        date_time = {"type": ["null", "string"], "format": "date-time"}
        _run(
            lake,
            [
                _schema(_properties(date_time)),
                *_records([{"id": 7, "v": "2024-01-01T00:00:00Z"}]),
            ],
        )
        _run(
            lake,
            [
                _schema(_properties(STRING)),
                *_records([{"id": 1, "v": "2024-01-02T03:04:05+02:00"}]),
            ],
        )
        assert _rows(lake)[0] == (1, "2024-01-02 01:04:05+00")
        with pytest.raises(ValueError, match="is TIMESTAMP WITH TIME ZONE, and 1"):
            _run(
                lake,
                [_schema(_properties(STRING)), *_records([{"id": 3, "v": "soon"}])],
            )


class TestTypeSourceKeepingText:
    """``on_error: string`` into an existing DATE column: checked per batch."""

    def test_a_date_loads_and_other_text_fails(self, lake: Lake):
        """A converted date casts; the raw text of one that isn't doesn't."""
        _run(
            lake,
            [_schema(_properties(STRING)), *_records([{"id": 7, "v": "2024-01-02"}])],
            stream_defaults=DATE_SOURCE,
        )
        keep_text = {"stream_defaults": {**DATE_SOURCE, "on_error": "string"}}
        _run(
            lake,
            [_schema(_properties(STRING)), *_records([{"id": 1, "v": "2024-02-03"}])],
            **keep_text,
        )
        with pytest.raises(ValueError, match=r"column 'v' is DATE, and 1 value"):
            _run(
                lake,
                [_schema(_properties(STRING)), *_records([{"id": 2, "v": "soon"}])],
                **keep_text,
            )
        assert _rows(lake) == [(1, "2024-02-03"), (7, "2024-01-02")]


class TestStillLoads:
    """Changes the table's column already holds, or that widen it in place."""

    @pytest.mark.parametrize("load_method", LOAD_METHODS)
    @pytest.mark.parametrize("kind", ["integer", "number", "boolean", "date"])
    def test_any_property_into_a_text_column(
        self, lake: Lake, kind: str, load_method: str
    ):
        """A VARCHAR column holds the text of any value."""
        _run(lake, [_schema(_properties(STRING)), *_records([{"id": 7, "v": "x"}])])
        schema, value, _, text = EARLIER[kind][:4]
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
        assert _column_type(lake) == [("BIGINT",)]

    def test_an_unsized_decimal_string_into_a_decimal_column(self, lake: Lake):
        """Staged at the column's own type, as before (see _staged_columns)."""
        sized = {"type": ["null", "number"], "precision": 10, "scale": 2}
        _run(lake, [_schema(_properties(sized)), *_records([{"id": 7, "v": 1.5}])])
        unsized = {"type": ["null", "string"], "format": "x-singer.decimal"}
        _run(
            lake,
            [_schema(_properties(unsized)), *_records([{"id": 1, "v": "2.25"}])],
        )
        assert _rows(lake) == [(1, "2.25"), (7, "1.50")]
