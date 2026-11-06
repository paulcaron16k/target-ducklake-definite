"""Native STRUCT and LIST columns (DUCKLKD-108, DUCKLKD-85, DUCKLKD-109)."""

from __future__ import annotations

import io
import json
import logging
import warnings
from datetime import datetime, timezone
from decimal import Decimal
from typing import TYPE_CHECKING, ClassVar
from unittest.mock import Mock

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from singer_sdk.exceptions import ConfigValidationError
from singer_sdk.sinks import SQLSink

from target_ducklake.connector import DuckLakeConnector
from target_ducklake.duckdb_types import (
    DuckDBList,
    DuckDBScalar,
    DuckDBStruct,
    parse_type,
    quote_identifier,
    render_type,
)
from target_ducklake.flatten import flatten_record, flatten_schema
from target_ducklake.layout import (
    LayoutError,
    check_layout_config,
    resolve_nested_types,
)
from target_ducklake.parquet_utils import (
    StagingRules,
    create_pyarrow_table,
    date_time_paths,
    nested_leaf_paths,
    nested_schema_changes,
    staging_type,
)
from target_ducklake.sinks import ducklakeSink
from target_ducklake.target import Targetducklake

if TYPE_CHECKING:
    from pathlib import Path

    from tests.catalogs import Lake

PAYLOAD = b"\x00\x01\xfe\xff"
PAYLOAD_TEXT = "AAH+/w=="


class TestParseType:
    """DuckDB type strings, as the DDL and PRAGMA table_info write them."""

    @pytest.mark.parametrize(
        ("text", "tree"),
        [
            ("INTEGER", DuckDBScalar("INTEGER")),
            ("DECIMAL(10,2)", DuckDBScalar("DECIMAL(10,2)")),
            ("decimal( 10 , 2 )", DuckDBScalar("decimal( 10 , 2 )")),
            (
                "TIMESTAMP  WITH TIME ZONE",
                DuckDBScalar("TIMESTAMP WITH TIME ZONE"),
            ),
            ("MAP(VARCHAR, INTEGER)", DuckDBScalar("MAP(VARCHAR, INTEGER)")),
            ("INTEGER[]", DuckDBList(DuckDBScalar("INTEGER"))),
            ("VARCHAR[][]", DuckDBList(DuckDBList(DuckDBScalar("VARCHAR")))),
            ("INTEGER[3]", DuckDBList(DuckDBScalar("INTEGER"), 3)),
            ("DECIMAL(38,9)[]", DuckDBList(DuckDBScalar("DECIMAL(38,9)"))),
            (
                'STRUCT(a INTEGER, "at" TIMESTAMP WITH TIME ZONE)[]',
                DuckDBList(
                    DuckDBStruct(
                        (
                            ("a", DuckDBScalar("INTEGER")),
                            ("at", DuckDBScalar("TIMESTAMP WITH TIME ZONE")),
                        )
                    )
                ),
            ),
            (
                'STRUCT("Sel ect" VARCHAR, "a""b" JSON, "x,y" DECIMAL(5,1))',
                DuckDBStruct(
                    (
                        ("Sel ect", DuckDBScalar("VARCHAR")),
                        ('a"b', DuckDBScalar("JSON")),
                        ("x,y", DuckDBScalar("DECIMAL(5,1)")),
                    )
                ),
            ),
            (
                "STRUCT(s STRUCT(l STRUCT(x DOUBLE)[][]), struct INTEGER)",
                DuckDBStruct(
                    (
                        (
                            "s",
                            DuckDBStruct(
                                (
                                    (
                                        "l",
                                        DuckDBList(
                                            DuckDBList(
                                                DuckDBStruct(
                                                    (("x", DuckDBScalar("DOUBLE")),)
                                                )
                                            )
                                        ),
                                    ),
                                )
                            ),
                        ),
                        ("struct", DuckDBScalar("INTEGER")),
                    )
                ),
            ),
        ],
        ids=[
            "scalar",
            "decimal",
            "decimal-spaces",
            "words",
            "map-is-scalar",
            "list",
            "list-of-lists",
            "fixed-size",
            "list-of-decimals",
            "pragma-list-of-struct",
            "quoted-names",
            "deep-and-reserved",
        ],
    )
    def test_parses(self, text: str, tree: object):
        """Each part parses at any depth; quoted names lose their quotes."""
        assert parse_type(text) == tree

    @pytest.mark.parametrize(
        "text",
        [
            "STRUCT(a INTEGER",
            "STRUCT()",
            "STRUCT(a INTEGER, A VARCHAR)",
            'STRUCT("a INTEGER)',
            "DECIMAL(10,2",
            "INTEGER) x",
            "",
        ],
        ids=[
            "unbalanced",
            "no-fields",
            "duplicate-field",
            "unterminated-quote",
            "unbalanced-scalar",
            "trailing-text",
            "empty",
        ],
    )
    def test_refuses_what_isnt_a_type(self, text: str):
        """A ValueError naming the text, not a wrong tree."""
        with pytest.raises(ValueError, match="Can't read DuckDB type"):
            parse_type(text)

    @pytest.mark.parametrize(
        ("text", "expected"),
        [
            (
                "STRUCT(a INTEGER, at TIMESTAMPTZ)[]",
                'STRUCT("a" INTEGER, "at" TIMESTAMPTZ)[]',
            ),
            (
                'STRUCT("a""b" STRUCT(c DECIMAL(10,2)[]), select JSON)',
                'STRUCT("a""b" STRUCT("c" DECIMAL(10,2)[]), "select" JSON)',
            ),
            ("BIGINT[][]", "BIGINT[][]"),
        ],
    )
    def test_renders_what_it_parses(self, text: str, expected: str):
        """Every field name quoted, and the DDL reads back as the same tree."""
        rendered = render_type(parse_type(text))
        assert rendered == expected
        assert parse_type(rendered) == parse_type(text)
        assert render_type(parse_type(rendered)) == rendered

    def test_rendered_types_create_tables_duckdb_reports_back(self):
        """DuckDB accepts the DDL, reserved names too, and PRAGMA reports the tree."""
        text = 'STRUCT("at" TIMESTAMPTZ, "select" INTEGER, "a""b" VARCHAR[])[]'
        con = duckdb.connect()
        con.execute(f"CREATE TABLE t (c {render_type(parse_type(text))})")
        reported = con.execute("PRAGMA table_info('t')").fetchall()[0][2]
        assert parse_type(reported) == DuckDBList(
            DuckDBStruct(
                (
                    ("at", DuckDBScalar("TIMESTAMP WITH TIME ZONE")),
                    ("select", DuckDBScalar("INTEGER")),
                    ('a"b', DuckDBList(DuckDBScalar("VARCHAR"))),
                )
            )
        )

    def test_quote_identifier(self):
        """A quote in a name is doubled."""
        assert quote_identifier('a"b') == '"a""b"'


class TestNestedStagingType:
    """Struct fields and list elements stage as their scalar types do."""

    def test_struct_and_list_are_built_from_their_parts(self):
        """Each field and element stages as a column of its type would."""
        staged = staging_type(
            'STRUCT(a INTEGER, "at" TIMESTAMP WITH TIME ZONE, d DECIMAL(10,2), '
            "b BLOB, j JSON, f DOUBLE, ok BOOLEAN, l BIGINT[], s STRUCT(x DOUBLE)[])"
        )
        assert staged == pa.struct(
            [
                ("a", pa.int64()),
                ("at", pa.string()),
                ("d", pa.decimal128(10, 2)),
                ("b", pa.binary()),
                ("j", pa.string()),
                ("f", pa.float64()),
                ("ok", pa.bool_()),
                ("l", pa.list_(pa.field("element", pa.int64()))),
                (
                    "s",
                    pa.list_(pa.field("element", pa.struct([("x", pa.float64())]))),
                ),
            ]
        )

    @pytest.mark.parametrize(
        ("text", "expected"),
        [
            ("INTEGER", pa.int64()),
            ("DECIMAL(10,2)", pa.decimal128(10, 2)),
            ("JSON", pa.string()),
            ("MAP(VARCHAR, INTEGER)", pa.string()),
            ("STRUCT(a", pa.string()),
        ],
    )
    def test_scalars_stage_as_before(self, text: str, expected: pa.DataType):
        """A type that isn't nested, or doesn't parse, stages as it did."""
        assert staging_type(text) == expected

    def test_leaf_paths(self):
        """Struct fields after a dot, list elements as ``[]``; scalars have none."""
        assert list(
            nested_leaf_paths("o", 'STRUCT(a INTEGER, i STRUCT("at" TIMESTAMPTZ)[])')
        ) == [("o.a", "INTEGER"), ("o.i[].at", "TIMESTAMPTZ")]
        assert list(nested_leaf_paths("n", "INTEGER")) == []

    def test_date_time_paths_follow_properties_and_items(self):
        """convert_tz_to_utc finds date-time leaves at any depth."""
        flat = {
            "at": {"type": "string", "format": "date-time"},
            "o": {
                "type": "object",
                "properties": {
                    "at": {"type": "string", "format": "date-time"},
                    "i": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "at": {"type": "string", "format": "date-time"},
                                "n": {"type": "integer"},
                            },
                        },
                    },
                },
            },
            "n": {"type": "integer"},
        }
        assert date_time_paths(flat) == ["at", "o.at", "o.i[].at"]


def _stage(column_type: str, values: list, **rules: object) -> list:
    """Stage one column ``c`` of the given DuckDB type; its values back."""
    schema = pa.schema([pa.field("c", staging_type(column_type))])
    datetime_fields = rules.pop("datetime_fields", [])
    table = create_pyarrow_table(
        [{"c": v} for v in values],
        schema,
        datetime_fields,  # type: ignore[arg-type]
        StagingRules(**rules),  # type: ignore[arg-type]
    )
    assert table.schema == schema
    return table.column("c").to_pylist()


class TestNestedStagingRules:
    """Every scalar rule applies at every depth, naming the path."""

    def test_decimal_rounding_is_warned_per_path(
        self, caplog: pytest.LogCaptureFixture
    ):
        """Rounded to the leaf's scale, counted under its path."""
        with caplog.at_level(logging.WARNING):
            staged = _stage(
                "STRUCT(items STRUCT(price DECIMAL(10,2))[])",
                [{"items": [{"price": Decimal("1.234")}, {"price": 2}]}],
            )
        assert staged == [
            {"items": [{"price": Decimal("1.23")}, {"price": Decimal("2.00")}]}
        ]
        assert (
            "1 value(s) in column 'c.items[].price' had more than 2 fraction digits"
            in caplog.text
        )

    def test_strict_rounding_fails_naming_the_path(self):
        """Under strict, the leaf's path and the value are named."""
        with pytest.raises(
            ValueError,
            match=r"Column 'c.items\[\].price' is DECIMAL\(10, 2\); Decimal\('1.234'\)"
            r" would be rounded",
        ):
            _stage(
                "STRUCT(items STRUCT(price DECIMAL(10,2))[])",
                [{"items": [{"price": Decimal("1.234")}]}],
                strict=True,
            )

    def test_a_value_too_large_fails_naming_the_path(self):
        """Too many integer digits fails either way."""
        with pytest.raises(ValueError, match=r"Column 'c.d' is DECIMAL\(3, 2\)"):
            _stage("STRUCT(d DECIMAL(3,2))", [{"d": Decimal("12.5")}])

    def test_integer_leaves_round_half_away_from_zero(
        self, caplog: pytest.LogCaptureFixture
    ):
        """As DuckDB's cast rounds; text that isn't a number fails."""
        with caplog.at_level(logging.WARNING):
            assert _stage("BIGINT[]", [[1, 2.5, Decimal("-2.5"), None]]) == [
                [1, 3, -3, None]
            ]
        assert "column 'c[]' had a fraction" in caplog.text
        with pytest.raises(ValueError, match=r"Column 'c\[\]' holds whole numbers"):
            _stage("BIGINT[]", [["x"]], strict=True)

    def test_float_leaves(self):
        """Decimals become floats; text that isn't a number fails."""
        assert _stage("STRUCT(f DOUBLE)", [{"f": Decimal("1.5e-12")}, {"f": 2}]) == [
            {"f": 1.5e-12},
            {"f": 2.0},
        ]
        with pytest.raises(ValueError, match="Column 'c.f' holds floating-point"):
            _stage("STRUCT(f DOUBLE)", [{"f": "abc"}])

    def test_boolean_leaves(self):
        """Booleans stay; anything else fails naming the path."""
        assert _stage("BOOLEAN[]", [[True, None, False]]) == [[True, None, False]]
        with pytest.raises(ValueError, match="Column 'c\\[\\]' holds booleans"):
            _stage("BOOLEAN[]", [["yes"]])

    def test_timestamptz_leaves_read_zone_less_values_as_utc_when_named(self):
        """The UTC rule of date_time_type: timestamptz, at depth."""
        values = [{"at": "2024-01-01T10:00:00"}, {"at": "2024-01-01T10:00:00+02:00"}]
        utc = _stage(
            'STRUCT("at" TIMESTAMPTZ)', values, utc_columns=frozenset({"c.at"})
        )
        assert utc == [
            {"at": "2024-01-01 10:00:00.000000Z"},
            {"at": "2024-01-01 08:00:00.000000Z"},
        ]
        # Not named (date_time_type: timestamp): the text is kept as sent.
        assert _stage('STRUCT("at" TIMESTAMPTZ)', values) == values

    def test_convert_tz_to_utc_reaches_nested_date_times(self):
        """Offset text moves to UTC; zone-less text is left as sent."""
        values = [[{"at": "2024-01-01T10:00:00+05:30"}, {"at": "2024-01-01T10:00:00"}]]
        assert _stage(
            'STRUCT("at" TIMESTAMP)[]', values, datetime_fields=["c[].at"]
        ) == [
            [
                {"at": "2024-01-01 04:30:00.000000Z"},
                {"at": "2024-01-01T10:00:00"},
            ]
        ]

    def test_datetimes_in_text_leaves_are_rendered_as_text(self):
        """An Arrow struct can't take a datetime where it stages text."""
        aware = datetime(2024, 1, 1, 10, tzinfo=timezone.utc)
        assert _stage("STRUCT(at TIMESTAMPTZ)", [{"at": aware}]) == [
            {"at": "2024-01-01 10:00:00.000000Z"}
        ]

    def test_blob_leaves_are_decoded_strictly(self):
        """As at the top level (DUCKLKD-91)."""
        assert _stage("STRUCT(b BLOB)[]", [[{"b": PAYLOAD_TEXT}, {"b": None}]]) == [
            [{"b": PAYLOAD}, {"b": None}]
        ]
        with pytest.raises(ValueError, match=r"Column 'c\[\].b' holds binary data"):
            _stage("STRUCT(b BLOB)[]", [[{"b": "not base64!"}]])

    def test_json_leaves_hold_json_text(self):
        """Every value of a JSON leaf, strings too; decimals keep their digits."""
        staged = _stage(
            "STRUCT(j JSON, l JSON[])",
            [
                {
                    "j": {"k": Decimal("1.10000000000000000001")},
                    "l": ["a", 1, None, [True]],
                },
                {"j": None, "l": None},
            ],
        )
        assert staged == [
            {
                "j": '{"k": 1.10000000000000000001}',
                "l": ['"a"', "1", None, "[true]"],
            },
            {"j": None, "l": None},
        ]

    def test_text_leaves_hold_scalars_as_text_and_objects_as_json(self):
        """As a top-level VARCHAR column holds them."""
        assert _stage("VARCHAR[]", [[True, Decimal("1.50"), {"a": 1}, "s"]]) == [
            ["true", "1.50", '{"a": 1}', "s"]
        ]


class TestNestedStagingShape:
    """Keys, NULLs and values of the wrong shape."""

    def test_undeclared_keys_are_dropped_with_a_count_per_path(
        self, caplog: pytest.LogCaptureFixture
    ):
        """DuckLake would drop them silently; the warning counts them."""
        with caplog.at_level(logging.WARNING):
            staged = _stage(
                "STRUCT(a INTEGER, i STRUCT(x INTEGER)[])",
                [
                    {"a": 1, "extra": 1, "i": [{"x": 1, "y": 2}, {"y": 3}]},
                    {"extra": 2, "other": 3},
                ],
            )
        assert staged == [
            {"a": 1, "i": [{"x": 1}, {"x": None}]},
            {"a": None, "i": None},
        ]
        assert (
            "Dropped 3 value(s) in column 'c' under key(s) its STRUCT doesn't "
            "declare: 'extra': 2, 'other': 1." in caplog.text
        )
        assert "Dropped 2 value(s) in column 'c.i[]'" in caplog.text

    def test_undeclared_keys_fail_under_strict(self):
        """Under strict, the path and the key are named."""
        with pytest.raises(
            ValueError,
            match=r"Column 'c.i\[\]' is a STRUCT without field 'y'.*strict",
        ):
            _stage(
                "STRUCT(i STRUCT(x INTEGER)[])",
                [{"i": [{"x": 1, "y": 2}]}],
                strict=True,
            )

    def test_keys_match_field_names_case_insensitively(self):
        """As DuckDB matches struct field names."""
        assert _stage("STRUCT(Name VARCHAR)", [{"nAME": "x"}]) == [{"Name": "x"}]

    def test_nulls_stay_null_never_a_struct_of_nulls(self):
        """NULL structs, NULL lists, empty lists and NULL elements."""
        staged = _stage(
            "STRUCT(a INTEGER)[]",
            [None, [], [None], [{"a": None}], [{"a": 1}, None]],
        )
        assert staged == [None, [], [None], [{"a": None}], [{"a": 1}, None]]
        assert _stage("STRUCT(s STRUCT(a INTEGER))", [None, {"s": None}]) == [
            None,
            {"s": None},
        ]

    @pytest.mark.parametrize(
        ("column_type", "value", "message"),
        [
            ("STRUCT(a INTEGER)", "text", r"Column 'c' holds objects; 'text' is not"),
            ("STRUCT(a INTEGER)", [1], r"Column 'c' holds objects; \[1\] is not"),
            ("INTEGER[]", {"a": 1}, r"Column 'c' holds lists; \{'a': 1\} is not"),
            ("STRUCT(l INTEGER[])", {"l": "1,2"}, r"Column 'c.l' holds lists"),
        ],
    )
    def test_a_value_of_the_wrong_shape_fails_naming_the_path(
        self, column_type: str, value: object, message: str
    ):
        """Not pyarrow's error, which names neither the column nor the value."""
        with pytest.raises(ValueError, match=message):
            _stage(column_type, [value])


class TestNestedStagingRoundTrip:
    """Staged through Parquet into DuckDB and DuckLake tables of the same type."""

    COLUMN_TYPE = (
        'STRUCT(id INTEGER, "at" TIMESTAMP WITH TIME ZONE, price DECIMAL(10,2), '
        '"select" VARCHAR, b BLOB, j JSON, tags VARCHAR[], '
        "items STRUCT(n BIGINT, f DOUBLE)[])"
    )
    VALUES = [  # noqa: RUF012
        {
            "id": 1,
            "at": "2024-01-01T10:00:00",
            "price": Decimal("9.99"),
            "select": "s",
            "b": PAYLOAD_TEXT,
            "j": {"k": [1, Decimal("2.5")]},
            "tags": ["x", None],
            "items": [{"n": 1, "f": 0.5}, None, {"n": None}],
        },
        {"id": 2, "items": []},
        None,
    ]
    EXPECTED = [  # noqa: RUF012
        (
            "{'id': 1, 'at': '2024-01-01 10:00:00+00', 'price': 9.99, 'select': s, "
            "'b': \\x00\\x01\\xFE\\xFF, 'j': '{\"k\": [1, 2.5]}', 'tags': [x, NULL], "
            "'items': [{'n': 1, 'f': 0.5}, NULL, {'n': NULL, 'f': NULL}]}"
        ),
        (
            "{'id': 2, 'at': NULL, 'price': NULL, 'select': NULL, 'b': NULL, "
            "'j': NULL, 'tags': NULL, 'items': []}"
        ),
        None,
    ]

    def _staged_file(self, tmp_path: Path) -> Path:
        schema = pa.schema(
            [pa.field("k", pa.int64()), pa.field("c", staging_type(self.COLUMN_TYPE))]
        )
        table = create_pyarrow_table(
            [{"k": i, "c": v} for i, v in enumerate(self.VALUES)],
            schema,
            rules=StagingRules(utc_columns=frozenset({"c.at"})),
        )
        path = tmp_path / "staged.parquet"
        pq.write_table(table, path)
        return path

    @staticmethod
    def _rows(con: duckdb.DuckDBPyConnection, table: str) -> list:
        # UTC output, so the expected text doesn't depend on the session zone;
        # the zone the values were read in is New York.
        con.execute("SET TimeZone = 'UTC'")
        return [
            row[0]
            for row in con.execute(
                f"SELECT c::VARCHAR FROM {table} ORDER BY k"  # noqa: S608
            ).fetchall()
        ]

    def test_into_duckdb(self, tmp_path: Path):
        """A plain DuckDB table of the same type, read under a non-UTC zone."""
        staged = self._staged_file(tmp_path)
        con = duckdb.connect()
        con.execute("SET TimeZone = 'America/New_York'")
        con.execute(f"CREATE TABLE t (k BIGINT, c {self.COLUMN_TYPE})")
        con.execute(f"INSERT INTO t SELECT k, c FROM '{staged}'")  # noqa: S608
        assert self._rows(con, "t") == self.EXPECTED

    def test_into_ducklake(self, tmp_path: Path, lake: Lake):
        """A DuckLake table on a SQLite or Postgres catalog, under a non-UTC zone."""
        staged = self._staged_file(tmp_path)
        con = duckdb.connect()
        con.execute("SET TimeZone = 'America/New_York'")
        con.execute(lake.attach_sql())
        con.execute(f"CREATE TABLE lake.t (k BIGINT, c {self.COLUMN_TYPE})")
        con.execute(f"INSERT INTO lake.t SELECT k, c FROM '{staged}'")  # noqa: S608
        assert self._rows(con, "lake.t") == self.EXPECTED


class TestNestedStagingReview:
    """Cases from the review of PR #111."""

    @pytest.mark.parametrize(
        ("value", "expected"),
        [("true", True), ("false", False), ("1", True), (0, False)],
    )
    def test_boolean_leaves_accept_what_a_boolean_column_does(
        self, value: object, expected: object
    ):
        """Finding 1: as the top-level BOOLEAN column's cast reads them."""
        top = create_pyarrow_table(
            [{"c": value}], pa.schema([pa.field("c", pa.bool_())])
        )
        assert top.column("c").to_pylist() == [expected]
        assert _stage("STRUCT(a BOOLEAN)", [{"a": value}]) == [{"a": expected}]

    def test_keys_differing_only_in_case_warn(self, caplog: pytest.LogCaptureFixture):
        """Finding 2: one value would be lost; permissive keeps the last, warning."""
        with caplog.at_level(logging.WARNING):
            assert _stage("STRUCT(a INTEGER)", [{"a": 1, "A": 2}]) == [{"a": 2}]
        assert "Column 'c' got keys 'a' and 'A' for field 'a'" in caplog.text

    def test_keys_differing_only_in_case_fail_under_strict(self):
        """Finding 2, strict: the path and both keys are named."""
        with pytest.raises(ValueError, match="Column 'c' got keys 'a' and 'A'"):
            _stage("STRUCT(a INTEGER)", [{"a": 1, "A": 2}], strict=True)

    @pytest.mark.parametrize(
        ("text", "leaf"),
        [
            ("STRUCT(a ENUM('x)', 'y,z'))", "ENUM('x)', 'y,z')"),
            (
                'STRUCT(a UNION("n)" INTEGER, s VARCHAR))',
                'UNION("n)" INTEGER, s VARCHAR)',
            ),
        ],
    )
    def test_quoted_text_in_a_scalar(self, text: str, leaf: str):
        """Finding 3: parentheses and commas inside quotes don't end the type."""
        assert parse_type(text) == DuckDBStruct((("a", DuckDBScalar(leaf)),))

    def test_a_fixed_size_array_keeps_its_size(self):
        """Finding 4: rendered DDL keeps an ARRAY an ARRAY."""
        assert render_type(parse_type("INTEGER[3][]")) == "INTEGER[3][]"

    def test_leaf_paths_are_distinct(self):
        """Finding 5: a name holding a dot is quoted in its path."""
        paths = [
            path
            for path, _ in nested_leaf_paths(
                "c", 'STRUCT("a.b" TIMESTAMPTZ, a STRUCT(b TIMESTAMPTZ))'
            )
        ]
        assert paths == ['c."a.b"', "c.a.b"]
        staged = _stage(
            'STRUCT("a.b" TIMESTAMPTZ, a STRUCT(b TIMESTAMPTZ))',
            [{"a.b": "2024-01-01T10:00:00", "a": {"b": "2024-01-01T10:00:00"}}],
            utc_columns=frozenset({"c.a.b"}),
        )
        assert staged == [
            {"a.b": "2024-01-01T10:00:00", "a": {"b": "2024-01-01 10:00:00.000000Z"}}
        ]

    def test_date_time_paths_quote_names_as_staging_does(self):
        """Round 2: convert_tz_to_utc finds a field whose name holds a dot."""
        dt = {"type": "string", "format": "date-time"}
        flat = {"c": {"type": "object", "properties": {"a.b": dt, "x": dt}}}
        column_type = 'STRUCT("a.b" TIMESTAMP, x TIMESTAMP)'
        paths = date_time_paths(flat)
        assert paths == [path for path, _ in nested_leaf_paths("c", column_type)]
        value = "2024-01-01T10:00:00+05:00"
        assert _stage(
            column_type, [{"a.b": value, "x": value}], datetime_fields=paths
        ) == [
            {"a.b": "2024-01-01 05:00:00.000000Z", "x": "2024-01-01 05:00:00.000000Z"}
        ]


# DUCKLKD-85: STRUCT and LIST columns for shaped objects and arrays.

DATE_TIME = {"type": ["null", "string"], "format": "date-time"}
NOON_UTC = 1704110400  # 2024-01-01T12:00:00Z


def _ddl(props: dict, **settings: object) -> dict[str, str]:
    """The column types the sink would create for these properties."""
    nested_types = str(settings.pop("nested_types", "native"))
    flat = flatten_schema(
        {"type": "object", "properties": props},
        max_level=int(settings.pop("flatten_max_level", 0)),  # type: ignore[call-overload]
        nested=nested_types == "native",
    )
    connector = DuckLakeConnector({"catalog_url": "x", "data_path": "/x"})
    columns = connector.json_to_ducklake_schema(
        flat, str(settings.pop("date_time_type", "timestamptz")), nested_types
    )
    return {column["name"]: column["type"] for column in columns}


class TestNestedColumnTypes:
    """The DDL of objects and arrays under nested_types (DUCKLKD-85)."""

    @pytest.mark.parametrize(
        ("prop", "expected"),
        [
            (
                {"type": "object", "properties": {"a": {"type": "integer"}}},
                'STRUCT("a" BIGINT)',
            ),
            ({"type": ["null", "array"], "items": {"type": "string"}}, "VARCHAR[]"),
            (
                {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "at": {"type": "string", "format": "date-time"},
                            "select": {"type": "boolean"},
                        },
                    },
                },
                'STRUCT("at" TIMESTAMPTZ, "select" BOOLEAN)[]',
            ),
            (
                {"type": "array", "items": {"type": "array", "items": DATE_TIME}},
                "TIMESTAMPTZ[][]",
            ),
            (
                {
                    "type": "object",
                    "properties": {
                        "hinted": {"type": "string", "x-sql-datatype": "uuid"},
                        "price": {"type": "number", "precision": 10, "scale": 2},
                        "sized": {
                            "type": "string",
                            "format": "x-singer.decimal",
                            "precision": 12,
                            "scale": 4,
                        },
                        "unsized": {"type": "string", "format": "x-singer.decimal"},
                        "b": {"type": "string", "contentEncoding": "base64"},
                        "n": {"type": "number"},
                        "free": {"type": "object"},
                        "untyped": {},
                        "several": {"type": ["integer", "string"]},
                        "choice": {"anyOf": [{"type": "integer"}, {"type": "null"}]},
                    },
                },
                'STRUCT("hinted" UUID, "price" DECIMAL(10, 2), "sized" '
                'DECIMAL(12, 4), "unsized" VARCHAR, "b" BLOB, "n" DECIMAL(38, 9), '
                '"free" JSON, "untyped" JSON, "several" VARCHAR, "choice" BIGINT)',
            ),
            (
                {"type": "object", "properties": {'a "q"': {"type": "integer"}}},
                'STRUCT("a ""q""" BIGINT)',
            ),
            ({"type": "object"}, "JSON"),
            ({"type": "object", "properties": {}}, "JSON"),
            ({"type": "array"}, "JSON"),
            ({"type": "array", "items": {}}, "JSON"),
            ({"type": "array", "items": {"type": "object"}}, "JSON[]"),
            (
                {
                    "type": ["object", "string"],
                    "properties": {"a": {"type": "integer"}},
                },
                "JSON",
            ),
            (
                {
                    "anyOf": [
                        {"type": "object", "properties": {"a": {"type": "integer"}}},
                        {"type": "null"},
                    ]
                },
                "JSON",
            ),
            (
                {"oneOf": [{"type": "array", "items": {"type": "integer"}}]},
                "JSON",
            ),
            (
                {
                    "type": "object",
                    "properties": {"a": {"type": "integer"}},
                    "x-sql-datatype": "jsonb",
                },
                "JSON",
            ),
        ],
        ids=[
            "struct",
            "list",
            "list-of-struct-reserved-names",
            "list-of-lists",
            "leaves-follow-the-scalar-rules",
            "quoted-name",
            "object-without-properties",
            "empty-properties",
            "array-without-items",
            "untyped-items",
            "items-without-properties",
            "several-types",
            "any-of",
            "one-of",
            "json-hint",
        ],
    )
    def test_native(self, prop: dict, expected: str):
        """STRUCT and T[] where the shape is declared; JSON otherwise."""
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            assert _ddl({"c": prop})["c"] == expected

    def test_the_ddl_creates_the_table_it_describes(self):
        """DuckDB accepts the DDL, reserved and quoted names included."""
        types = _ddl(
            {
                "o": {
                    "type": "object",
                    "properties": {
                        "at": DATE_TIME,
                        "order": {"type": "array", "items": {"type": "integer"}},
                        'a"b': {"type": "string"},
                    },
                }
            }
        )
        con = duckdb.connect()
        con.execute(f'CREATE TABLE t ("o" {types["o"]})')
        reported = con.execute("PRAGMA table_info('t')").fetchall()[0][2]
        assert reported == (
            'STRUCT("at" TIMESTAMP WITH TIME ZONE, "order" BIGINT[], "a""b" VARCHAR)'
        )

    def test_date_time_type_reaches_leaves(self):
        """date_time_type: timestamp gives TIMESTAMP leaves (DATETIME, its alias)."""
        prop = {"type": "object", "properties": {"at": DATE_TIME}}
        ddl = _ddl({"o": prop}, date_time_type="timestamp")["o"]
        assert ddl == 'STRUCT("at" DATETIME)'
        con = duckdb.connect()
        con.execute(f"CREATE TABLE t (o {ddl})")
        assert con.execute("PRAGMA table_info('t')").fetchall()[0][2] == (
            'STRUCT("at" TIMESTAMP)'
        )

    def test_json_keeps_everything_nested_as_json(self):
        """nested_types: json, as before."""
        props = {
            "o": {"type": "object", "properties": {"a": {"type": "integer"}}},
            "l": {"type": "array", "items": {"type": "integer"}},
        }
        assert _ddl(props, nested_types="json") == {"l": "JSON", "o": "JSON"}

    def test_flatten_max_level_flattens_first(self):
        """Objects above flatten_max_level become columns; deeper ones STRUCT."""
        props = {
            "o": {
                "type": "object",
                "properties": {
                    "a": {"type": "integer"},
                    "p": {"type": "object", "properties": {"b": {"type": "integer"}}},
                    "l": {"type": "array", "items": {"type": "integer"}},
                },
            }
        }
        assert _ddl(props, flatten_max_level=1) == {
            "o__a": "BIGINT",
            "o__l": "BIGINT[]",
            "o__p": 'STRUCT("b" BIGINT)',
        }

    def test_nested_warnings_name_the_path(self, caplog: pytest.LogCaptureFixture):
        """An unsized x-singer.decimal leaf is named by its path."""
        prop = {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "price": {"type": "string", "format": "x-singer.decimal"}
                },
            },
        }
        with caplog.at_level(logging.WARNING):
            _ddl({"o": prop})
        assert "Property 'o[].price' is an x-singer.decimal string" in caplog.text


class TestNestedFlattening:
    """flatten_schema keeps the shape; flatten_record keeps dicts and lists."""

    def test_flatten_schema_keeps_properties_and_items(self):
        """Normalised at every depth: descriptions dropped, choices resolved."""
        flat = flatten_schema(
            {
                "type": "object",
                "properties": {
                    "o": {
                        "type": "object",
                        "description": "d",
                        "properties": {
                            "c": {"anyOf": [{"type": "string"}, {"type": "null"}]},
                            "l": {"type": "array", "items": {"type": "integer"}},
                        },
                    }
                },
            },
            nested=True,
        )
        assert flat == {
            "o": {
                "type": ["null", "object"],
                "properties": {
                    "c": {"type": ["null", "string"]},
                    "l": {"type": ["null", "array"], "items": {"type": "integer"}},
                },
            }
        }

    def test_without_nested_the_shape_is_dropped(self):
        """As before: an object column is JSON."""
        flat = flatten_schema(
            {
                "type": "object",
                "properties": {
                    "o": {"type": "object", "properties": {"a": {"type": "integer"}}}
                },
            }
        )
        assert flat == {"o": {"type": ["null", "object"]}}

    def test_flatten_record_keeps_dicts_and_lists_of_native_columns(self):
        """Other objects and lists are still JSON text."""
        record = {"o": {"a": 1}, "l": [1], "j": {"a": 1}}
        assert flatten_record(record, native_columns=frozenset({"o", "l"})) == {
            "o": {"a": 1},
            "l": [1],
            "j": '{"a": 1}',
        }


class TestNestedTypesSetting:
    """nested_types at the top level and per stream, as date_time_type is."""

    @pytest.mark.parametrize(
        ("config", "expected"),
        [
            ({}, "native"),
            ({"nested_types": "json"}, "json"),
            (
                {"nested_types": "json", "stream_defaults": {"nested_types": "native"}},
                "native",
            ),
            (
                {
                    "stream_defaults": {"nested_types": "json"},
                    "stream_overrides": {"t": {"nested_types": "native"}},
                },
                "native",
            ),
            ({"stream_overrides": {"other": {"nested_types": "json"}}}, "native"),
        ],
        ids=["default", "top-level", "defaults", "override", "other-stream"],
    )
    def test_precedence(self, config: dict, expected: str):
        """Override, then stream_defaults, then the top level, then native."""
        assert resolve_nested_types(config, "t") == expected

    def test_a_bad_stream_value_is_rejected_at_startup(self):
        """Every stream's value is checked, even one whose records never come."""
        config = {"stream_overrides": {"t": {"nested_types": "struct"}}}
        with pytest.raises(LayoutError, match=r"nested_types must be one of"):
            check_layout_config(config)

    def test_the_top_level_value_is_checked_by_the_config_schema(self):
        """An unknown top-level value fails config validation."""
        with pytest.raises(ConfigValidationError):
            Targetducklake(
                config={
                    "catalog_url": "x",
                    "data_path": "/x",
                    "nested_types": "struct",
                },
                validate_config=True,
            )


class TestNestedColumnsThroughListen:
    """Native columns through listen() on a real SQLite catalog (DUCKLKD-85).

    The target's DuckDB session is in Tokyo, so a naive date-time read in the
    session zone, rather than as UTC, shows.
    """

    PROPS: ClassVar[dict] = {
        "o": {
            "type": ["null", "object"],
            "properties": {
                "at": DATE_TIME,
                "price": {"type": "number", "precision": 10, "scale": 2},
                "select": {"type": "string"},
                "b": {"type": "string", "contentEncoding": "base64"},
                "free": {"type": "object"},
                "items": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {"n": {"type": "integer"}, "at": DATE_TIME},
                    },
                },
            },
        },
        "tags": {"type": ["null", "array"], "items": {"type": "string"}},
        "v": {"type": "string"},
    }
    RECORDS: ClassVar[list[dict]] = [
        {
            "id": 1,
            "o": {
                "at": "2024-01-01T12:00:00",
                "price": 1.5,
                "select": "x",
                "b": PAYLOAD_TEXT,
                "free": {"k": [1, 2.25]},
                "items": [{"n": 1, "at": "2024-01-01T21:00:00+09:00"}, None],
            },
            "tags": ["a", None],
            "v": "first",
        },
        {"id": 2, "o": None, "tags": [], "v": "second"},
        {"id": 3, "o": {"select": "only"}, "tags": None, "v": "third"},
    ]

    @staticmethod
    def _run(
        lake: Lake,
        props: dict,
        records: list[dict],
        monkeypatch: pytest.MonkeyPatch,
        key_properties: list[str] | None = None,
        **config: object,
    ) -> None:
        startup = DuckLakeConnector._build_startup_script
        monkeypatch.setattr(
            DuckLakeConnector,
            "_build_startup_script",
            lambda self: startup(self) + "\nSET TimeZone = 'Asia/Tokyo';",
        )
        full_config = {**lake.config(), **config}
        schema = {"type": "object", "properties": {"id": {"type": "integer"}, **props}}
        messages = [
            {
                "type": "SCHEMA",
                "stream": "t",
                "schema": schema,
                "key_properties": ["id"] if key_properties is None else key_properties,
            },
            *({"type": "RECORD", "stream": "t", "record": r} for r in records),
        ]
        target = Targetducklake(config=full_config)
        try:
            target.listen(
                file_input=io.StringIO("".join(json.dumps(m) + "\n" for m in messages))
            )
        finally:
            target.target_connector.close()

    @staticmethod
    def _query(lake: Lake, sql: str) -> list[tuple]:
        return lake.query(sql)

    def _values(self, lake: Lake) -> list[tuple]:
        return self._query(
            lake,
            "SELECT id, epoch(o.at), o.price, o.select, o.b, o.free, "
            "[struct_pack(n := i.n, at := epoch(i.at)) for i in o.items], "
            "tags, typeof(o), typeof(tags) FROM lake.s.t ORDER BY id, v",
        )

    EXPECTED: ClassVar[list[tuple]] = [
        (
            1,
            NOON_UTC,
            Decimal("1.50"),
            "x",
            PAYLOAD,
            '{"k": [1, 2.25]}',
            [{"n": 1, "at": NOON_UTC}, {"n": None, "at": None}],
            ["a", None],
        ),
        (2, None, None, None, None, None, None, []),
        (3, None, None, "only", None, None, None, None),
    ]
    O_TYPE = (
        'STRUCT("at" TIMESTAMP WITH TIME ZONE, price DECIMAL(10,2), "select" '
        "VARCHAR, b BLOB, free JSON, items STRUCT(n BIGINT, "
        '"at" TIMESTAMP WITH TIME ZONE)[])'
    )

    @pytest.mark.parametrize("load_method", ["append", "merge", "overwrite"])
    def test_round_trip(
        self, lake: Lake, monkeypatch: pytest.MonkeyPatch, load_method: str
    ):
        """Every leaf keeps its value; a second run adds rows only for append.

        A list comprehension maps a NULL element to a struct of NULLs; the
        stored element is NULL (see TestNestedStagingShape).
        """
        for _ in range(2):
            self._run(
                lake,
                self.PROPS,
                self.RECORDS,
                monkeypatch,
                load_method=load_method,
            )
        rows = self._values(lake)
        runs = 2 if load_method == "append" else 1
        expected = [
            (*row, self.O_TYPE, "VARCHAR[]")
            for row in self.EXPECTED
            for _ in range(runs)
        ]
        assert rows == expected
        assert (
            self._query(lake, "SELECT o.items[2] IS NULL FROM lake.s.t WHERE id = 1")
            == [(True,)] * runs
        )

    def test_the_last_record_of_a_key_is_kept(
        self, lake: Lake, monkeypatch: pytest.MonkeyPatch
    ):
        """The in-batch dedup works on a table with nested columns."""
        records = [
            {"id": 1, "o": {"select": "old"}, "tags": ["old"], "v": "a"},
            {"id": 1, "o": {"select": "new"}, "tags": ["new"], "v": "b"},
        ]
        self._run(lake, self.PROPS, records, monkeypatch)
        assert self._query(lake, "SELECT o.select, tags, v FROM lake.s.t") == [
            ("new", ["new"], "b")
        ]

    def test_an_existing_json_column_stays_json(
        self, lake: Lake, monkeypatch: pytest.MonkeyPatch
    ):
        """A table created with nested_types: json keeps JSON text columns."""
        records = [{"id": 1, "o": {"select": "x", "extra": 1}, "tags": ["a"], "v": "a"}]
        self._run(lake, self.PROPS, records, monkeypatch, nested_types="json")
        records = [{"id": 2, "o": {"select": "y"}, "tags": ["b"], "v": "b"}]
        self._run(lake, self.PROPS, records, monkeypatch)
        assert self._query(
            lake,
            "SELECT id, typeof(o), o->>'select', typeof(tags), tags->>0 "
            "FROM lake.s.t ORDER BY id",
        ) == [(1, "JSON", "x", "JSON", "a"), (2, "JSON", "y", "JSON", "b")]

    def test_a_new_column_is_added_as_a_struct(
        self, lake: Lake, monkeypatch: pytest.MonkeyPatch
    ):
        """An object property new to an existing table is added natively."""
        self._run(lake, {"v": {"type": "string"}}, [{"id": 1, "v": "a"}], monkeypatch)
        self._run(
            lake,
            self.PROPS,
            [{"id": 2, "o": {"select": "y"}, "v": "b"}],
            monkeypatch,
        )
        assert self._query(
            lake, "SELECT id, o.select, typeof(o) FROM lake.s.t ORDER BY id"
        ) == [(1, None, self.O_TYPE), (2, "y", self.O_TYPE)]

    def test_flattened_parents_with_struct_children(
        self, lake: Lake, monkeypatch: pytest.MonkeyPatch
    ):
        """flatten_max_level flattens first; the deeper object is a STRUCT."""
        props = {
            "p": {
                "type": "object",
                "properties": {
                    "a": {"type": "integer"},
                    "q": {"type": "object", "properties": {"at": DATE_TIME}},
                },
            }
        }
        record = {"id": 1, "p": {"a": 5, "q": {"at": "2024-01-01T12:00:00"}}}
        self._run(lake, props, [record], monkeypatch, flatten_max_level=1)
        assert self._query(
            lake, "SELECT p__a, epoch(p__q.at), typeof(p__q) FROM lake.s.t"
        ) == [(5, NOON_UTC, 'STRUCT("at" TIMESTAMP WITH TIME ZONE)')]

    def test_postgres_truncated_names_keep_their_values(
        self, monkeypatch: pytest.MonkeyPatch
    ):
        """A truncated nested column still keeps its dicts (Postgres catalogs)."""
        long_name = "o" * 70
        props = {
            long_name: {"type": "object", "properties": {"a": {"type": "integer"}}}
        }
        target = Mock(write_ledger=None)
        target.config = {
            "catalog_url": "x",
            "data_path": "/x",
            "catalog_type": "postgres",
            "default_target_schema": "s",
        }
        connector = DuckLakeConnector(target.config)
        sink = ducklakeSink(
            target=target,
            stream_name="t",
            schema={"type": "object", "properties": props},
            key_properties=[],
            connector=connector,
        )
        truncated = long_name[:63]
        monkeypatch.setattr(connector, "prepare_target_schema", lambda **_: None)
        monkeypatch.setattr(connector, "existing_column_types", lambda *_: {})
        monkeypatch.setattr(
            connector,
            "prepare_table",
            lambda **_: {truncated: 'STRUCT("a" BIGINT)'},
        )
        sink.setup()
        captured: list[dict] = []
        monkeypatch.setattr(
            SQLSink,
            "process_record",
            lambda _self, record, _ctx: captured.append(record),
        )
        sink.process_record({long_name: {"a": 1}}, {})
        assert captured == [{truncated: {"a": 1}}]


class TestNestedColumnRefusals:
    """Keys, partitions and sorts on STRUCT and LIST columns fail at startup."""

    PROPS: ClassVar[dict] = {
        "o": {"type": "object", "properties": {"a": {"type": "integer"}}},
        "l": {"type": "array", "items": {"type": "integer"}},
    }

    def _start(
        self,
        lake: Lake,
        monkeypatch: pytest.MonkeyPatch,
        key_properties: list[str],
        **config: object,
    ) -> None:
        TestNestedColumnsThroughListen._run(
            lake,
            self.PROPS,
            [],
            monkeypatch,
            key_properties=key_properties,
            **config,
        )

    @pytest.mark.parametrize("key", ["o", "l"])
    def test_a_nested_key_is_refused(
        self, lake: Lake, monkeypatch: pytest.MonkeyPatch, key: str
    ):
        """Named, with the fix."""
        with pytest.raises(
            ValueError, match=rf"key property '{key}' is a STRUCT or LIST column"
        ):
            self._start(lake, monkeypatch, [key])

    def test_a_key_on_a_json_column_still_loads(
        self, lake: Lake, monkeypatch: pytest.MonkeyPatch
    ):
        """nested_types: json keeps the earlier behaviour."""
        self._start(lake, monkeypatch, ["o"], nested_types="json")

    @pytest.mark.parametrize(
        ("layout", "kind", "column"),
        [
            ({"partition": [{"column": "o"}]}, "partition", "o"),
            ({"sort_order": ["l"]}, "sort", "l"),
        ],
    )
    def test_a_partition_or_sort_on_a_nested_column_is_refused(
        self,
        lake: Lake,
        monkeypatch: pytest.MonkeyPatch,
        layout: dict,
        kind: str,
        column: str,
    ):
        """Before the layout reaches the table."""
        with pytest.raises(
            LayoutError, match=rf"{kind} on column '{column}', a STRUCT or LIST"
        ):
            self._start(lake, monkeypatch, ["id"], stream_overrides={"t": layout})


# DUCKLKD-109: nested schema changes on existing STRUCT and LIST columns.


class TestNestedSchemaChangesUnit:
    """How a stream's nested column is compared with the table's."""

    def test_new_fields_are_additions_at_every_depth(self):
        """Struct fields, list-of-struct fields and deeper; matched by any case."""
        changes = nested_schema_changes(
            "o",
            'STRUCT(A INTEGER, b VARCHAR, "at" TIMESTAMPTZ, '
            "i STRUCT(n BIGINT, m BIGINT)[], d STRUCT(s STRUCT(q INTEGER, r JSON)))",
            "STRUCT(a INTEGER, i STRUCT(n BIGINT)[], d STRUCT(s STRUCT(q INTEGER)), "
            "gone VARCHAR)",
        )
        assert [(a.path, a.sql_path, a.column_type) for a in changes.additions] == [
            ("o.b", '"o"."b"', "VARCHAR"),
            ("o.at", '"o"."at"', "TIMESTAMPTZ"),
            ("o.i[].m", '"o"."i"."element"."m"', "BIGINT"),
            ("o.d.s.r", '"o"."d"."s"."r"', "JSON"),
        ]
        assert changes.conflicts == ()

    @pytest.mark.parametrize(
        ("stream", "table", "fits"),
        [
            ("BIGINT", "DECIMAL(10,2)", True),
            ("DECIMAL(38, 9)", "BIGINT", True),
            ("DECIMAL(38, 9)", "DOUBLE", True),
            ("INTEGER", "BIGINT", True),
            ("BIGINT", "VARCHAR", True),
            ("BOOLEAN", "JSON", True),
            ('STRUCT("x" BIGINT)', "JSON", True),
            ("VARCHAR", "TIMESTAMP WITH TIME ZONE", True),
            ("TIMESTAMPTZ", "TIMESTAMP", True),
            ("BLOB", "BLOB", True),
            ("VARCHAR", "BIGINT", False),
            ("VARCHAR", "DECIMAL(10,2)", False),
            ("VARCHAR", "BOOLEAN", False),
            ("VARCHAR", "BLOB", False),
            ("BIGINT", "TIMESTAMP WITH TIME ZONE", False),
            ("BOOLEAN", "BIGINT", False),
            ("JSON", 'STRUCT("x" BIGINT)', False),
            ('STRUCT("x" BIGINT)', "BIGINT", False),
            ("BIGINT[]", 'STRUCT("x" BIGINT)', False),
        ],
    )
    def test_leaf_type_changes(self, stream: str, table: str, fits: object):
        """Staged at the table's type where the scalar rules keep the meaning."""
        changes = nested_schema_changes(
            "o", f'STRUCT("f" {stream})', f'STRUCT("f" {table})'
        )
        assert changes.additions == ()
        if fits:
            assert changes.conflicts == ()
        else:
            assert [
                (c.path, c.table_type, c.stream_type) for c in changes.conflicts
            ] == [
                ("o.f", render_type(parse_type(table)), render_type(parse_type(stream)))
            ]


def _object(**fields: dict) -> dict:
    return {"type": ["null", "object"], "properties": fields}


INTEGER = {"type": ["null", "integer"]}
STRING = {"type": ["null", "string"]}


def test_each_added_field_is_logged_and_altered_in_place(
    caplog: pytest.LogCaptureFixture,
):
    """One ADD COLUMN per field, by its quoted path, and a log line naming it."""
    connector = DuckLakeConnector({"catalog_url": "x", "data_path": "/x"})
    executed: list[str] = []
    connector.execute = executed.append  # type: ignore[method-assign,assignment]
    changes = nested_schema_changes(
        "o",
        "STRUCT(a INTEGER, i STRUCT(m BIGINT)[])",
        "STRUCT(a INTEGER, i STRUCT(n BIGINT)[])",
    )
    with caplog.at_level(logging.INFO, logger="target_ducklake.connector"):
        connector._add_nested_fields("s", "t", list(changes.additions))
    assert executed == [
        'ALTER TABLE ducklake_catalog.s.t ADD COLUMN "o"."i"."element"."m" BIGINT;'
    ]
    assert "Adding field o.i[].m (BIGINT) to table t" in caplog.text


class TestNestedSchemaChangesThroughListen:
    """Existing STRUCT and LIST columns through listen() (DUCKLKD-109)."""

    @staticmethod
    def _run(
        lake: Lake,
        props: dict,
        records: list[dict],
        monkeypatch: pytest.MonkeyPatch,
        **config: object,
    ) -> None:
        TestNestedColumnsThroughListen._run(
            lake, props, records, monkeypatch, None, **config
        )

    @staticmethod
    def _type(lake: Lake, column: str) -> str:
        rows = lake.query("PRAGMA table_info('lake.s.t')")
        return {row[1]: row[2] for row in rows}[column]

    def test_new_sub_properties_are_added_in_place(
        self, lake: Lake, monkeypatch: pytest.MonkeyPatch
    ):
        """In a STRUCT and in a LIST of STRUCT; rows already loaded get NULL."""
        before = {
            "o": _object(a=INTEGER),
            "ls": {"type": "array", "items": _object(x=INTEGER)},
        }
        self._run(
            lake, before, [{"id": 1, "o": {"a": 1}, "ls": [{"x": 1}]}], monkeypatch
        )
        list_files = (
            "SELECT data_file FROM ducklake_list_files('lake', 't', schema => 's')"
        )
        (first_file,) = lake.query(list_files)
        after = {
            "o": _object(a=INTEGER, at=DATE_TIME),
            "ls": {"type": "array", "items": _object(x=INTEGER, y=STRING)},
        }
        record = {
            "id": 2,
            "o": {"a": 2, "at": "2024-01-01T12:00:00"},
            "ls": [{"x": 2, "y": "b"}],
        }
        self._run(lake, after, [record], monkeypatch)
        assert (
            self._type(lake, "o") == 'STRUCT(a BIGINT, "at" TIMESTAMP WITH TIME ZONE)'
        )
        assert self._type(lake, "ls") == "STRUCT(x BIGINT, y VARCHAR)[]"
        assert lake.query(
            "SELECT id, o.a, epoch(o.at), ls FROM lake.s.t ORDER BY id"
        ) == [
            (1, 1, None, [{"x": 1, "y": None}]),
            (2, 2, NOON_UTC, [{"x": 2, "y": "b"}]),
        ]
        # Metadata only: the first run's file is still the table's, beside the
        # second run's; adding the fields rewrote nothing.
        files = lake.query(list_files)
        assert first_file in files
        assert len(files) == 2

    def test_a_sub_property_no_longer_declared_stays_and_gets_null(
        self, lake: Lake, monkeypatch: pytest.MonkeyPatch
    ):
        """The table keeps the field; new rows have NULL in it."""
        self._run(
            lake,
            {"o": _object(a=INTEGER, b=STRING)},
            [{"id": 1, "o": {"a": 1, "b": "x"}}],
            monkeypatch,
        )
        self._run(
            lake, {"o": _object(a=INTEGER)}, [{"id": 2, "o": {"a": 2}}], monkeypatch
        )
        assert self._type(lake, "o") == "STRUCT(a BIGINT, b VARCHAR)"
        assert lake.query("SELECT id, o.a, o.b FROM lake.s.t ORDER BY id") == [
            (1, 1, "x"),
            (2, 2, None),
        ]

    def test_an_integer_into_an_existing_decimal_field_is_staged_at_its_type(
        self, lake: Lake, monkeypatch: pytest.MonkeyPatch
    ):
        """The field keeps DECIMAL(10, 2); the value is exact."""
        price = {"type": "number", "precision": 10, "scale": 2}
        self._run(
            lake, {"o": _object(p=price)}, [{"id": 1, "o": {"p": 1.25}}], monkeypatch
        )
        self._run(
            lake, {"o": _object(p=INTEGER)}, [{"id": 2, "o": {"p": 7}}], monkeypatch
        )
        assert self._type(lake, "o") == "STRUCT(p DECIMAL(10,2))"
        assert lake.query("SELECT id, o.p FROM lake.s.t ORDER BY id") == [
            (1, Decimal("1.25")),
            (2, Decimal("7.00")),
        ]

    def test_a_type_staging_cant_follow_is_refused_naming_field_and_types(
        self, lake: Lake, monkeypatch: pytest.MonkeyPatch
    ):
        """At startup, before any field is added: the table is unchanged."""
        self._run(
            lake, {"o": _object(n=INTEGER)}, [{"id": 1, "o": {"n": 1}}], monkeypatch
        )
        with pytest.raises(
            ValueError,
            match=r"field 'o.n' is BIGINT in the table, but the stream declares "
            r"VARCHAR",
        ):
            self._run(
                lake,
                {"o": _object(n=STRING, new=INTEGER)},
                [{"id": 2, "o": {"n": "x"}}],
                monkeypatch,
            )
        assert self._type(lake, "o") == "STRUCT(n BIGINT)"
        assert lake.query("SELECT id FROM lake.s.t") == [(1,)]

    @pytest.mark.parametrize(
        ("prop", "config"),
        [
            ({"type": ["null", "object"]}, {}),
            (_object(n=INTEGER), {"nested_types": "json"}),
            ({"type": ["null", "array"], "items": INTEGER}, {}),
        ],
        ids=["object-without-properties", "nested-types-json", "now-a-list"],
    )
    def test_a_struct_whose_shape_is_gone_is_refused(
        self, lake: Lake, monkeypatch: pytest.MonkeyPatch, prop: dict, config: dict
    ):
        """Named, at startup, rather than failing at staging."""
        self._run(
            lake, {"o": _object(n=INTEGER)}, [{"id": 1, "o": {"n": 1}}], monkeypatch
        )
        with pytest.raises(ValueError, match=r"Table t: (column|field) 'o'"):
            self._run(
                lake, {"o": prop}, [{"id": 2, "o": {"n": 2}}], monkeypatch, **config
            )
        assert lake.query("SELECT id FROM lake.s.t") == [(1,)]

    @pytest.mark.parametrize(
        ("prop", "config"),
        [
            ({"type": ["null", "array"]}, {}),
            ({"type": "array", "items": _object(x=INTEGER)}, {"nested_types": "json"}),
        ],
        ids=["array-without-items", "nested-types-json"],
    )
    def test_a_list_whose_items_are_gone_is_refused(
        self, lake: Lake, monkeypatch: pytest.MonkeyPatch, prop: dict, config: dict
    ):
        """As for a STRUCT: named, at startup, the table unchanged."""
        items = {"type": "array", "items": _object(x=INTEGER)}
        self._run(lake, {"ls": items}, [{"id": 1, "ls": [{"x": 1}]}], monkeypatch)
        with pytest.raises(ValueError, match=r"Table t: column 'ls' is STRUCT"):
            self._run(
                lake, {"ls": prop}, [{"id": 2, "ls": [{"x": 2}]}], monkeypatch, **config
            )
        assert self._type(lake, "ls") == "STRUCT(x BIGINT)[]"
        assert lake.query("SELECT id FROM lake.s.t") == [(1,)]

    def test_a_long_top_level_struct_name_gets_new_fields(
        self, lake: Lake, monkeypatch: pytest.MonkeyPatch
    ):
        """On Postgres the column's name is truncated to 63; fields are added to it."""
        name = "s" * 70
        column = name[:63] if lake.kind == "postgres" else name
        self._run(
            lake, {name: _object(a=INTEGER)}, [{"id": 1, name: {"a": 1}}], monkeypatch
        )
        self._run(
            lake,
            {name: _object(a=INTEGER, b=STRING)},
            [{"id": 2, name: {"a": 2, "b": "y"}}],
            monkeypatch,
        )
        assert self._type(lake, column) == "STRUCT(a BIGINT, b VARCHAR)"
        assert lake.query(
            f'SELECT id, "{column}".a, "{column}".b FROM lake.s.t ORDER BY id'  # noqa: S608
        ) == [(1, 1, None), (2, 2, "y")]

    @pytest.mark.parametrize("load_method", ["append", "merge", "overwrite"])
    def test_each_load_method_after_a_field_is_added(
        self, lake: Lake, monkeypatch: pytest.MonkeyPatch, load_method: str
    ):
        """The added field loads with every method."""
        self._run(
            lake,
            {"o": _object(a=INTEGER)},
            [{"id": 1, "o": {"a": 1}}],
            monkeypatch,
            load_method=load_method,
        )
        self._run(
            lake,
            {"o": _object(a=INTEGER, b=STRING)},
            [{"id": 1, "o": {"a": 2, "b": "y"}}],
            monkeypatch,
            load_method=load_method,
        )
        rows = lake.query("SELECT id, o.a, o.b FROM lake.s.t ORDER BY o.a")
        if load_method == "append":
            assert rows == [(1, 1, None), (1, 2, "y")]
        else:
            assert rows == [(1, 2, "y")]

    def test_long_nested_names_are_kept_on_postgres(
        self, lake: Lake, monkeypatch: pytest.MonkeyPatch
    ):
        """Nested names aren't Postgres identifiers: 70 characters load as sent.

        Only top-level names are truncated (max_column_length, ducklake#619).
        """
        long = "f" * 70
        self._run(
            lake,
            {"o": _object(**{long: INTEGER})},
            [{"id": 1, "o": {long: 1}}],
            monkeypatch,
        )
        self._run(
            lake,
            {"o": _object(**{long: INTEGER, "g" * 70: INTEGER})},
            [{"id": 2, "o": {long: 2, "g" * 70: 3}}],
            monkeypatch,
        )
        assert lake.query(
            f'SELECT id, o."{long}", o."{"g" * 70}" FROM lake.s.t ORDER BY id'  # noqa: S608
        ) == [(1, 1, None), (2, 2, 3)]


class TestNestedColumnsReview:
    """Cases from the review of PR #112."""

    @pytest.mark.parametrize(
        ("properties", "column_type"),
        [
            ({"a": {"type": "string"}, "A": {"type": "string"}}, "JSON"),
            ({"": {"type": "string"}}, "JSON"),
            (
                {"o": {"type": "object", "properties": {"b": {}, "B": {}}}},
                "STRUCT(o JSON)",
            ),
        ],
        ids=["case-collision", "empty-name", "nested-collision"],
    )
    def test_names_a_struct_cant_hold_stay_json(
        self,
        lake: Lake,
        monkeypatch: pytest.MonkeyPatch,
        properties: dict,
        column_type: str,
    ):
        """Finding 1: names that collide case-insensitively, or are empty, load as JSON.

        DuckDB refuses them in a STRUCT; before nested types they loaded as JSON.
        """
        props = {"o": {"type": ["null", "object"], "properties": properties}}
        record = {"id": 1, "o": dict.fromkeys(properties, "x")}
        TestNestedColumnsThroughListen._run(lake, props, [record], monkeypatch)
        rows = TestNestedColumnsThroughListen._query(
            lake, "SELECT typeof(o) FROM lake.s.t"
        )
        assert rows == [(column_type,)]

    def test_choice_columns_keep_their_shape_keywords_under_json(self):
        """Finding 3: with nested_types json, flatten_schema is as before."""
        prop = {"anyOf": [{"type": "object", "properties": {"a": {"type": "integer"}}}]}
        flat = flatten_schema({"type": "object", "properties": {"c": prop}})
        assert flat["c"]["properties"] == {"a": {"type": "integer"}}
