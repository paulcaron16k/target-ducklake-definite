"""Native STRUCT and LIST columns (DUCKLKD-108, DUCKLKD-85, DUCKLKD-109)."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import TYPE_CHECKING

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from target_ducklake.duckdb_types import (
    DuckDBList,
    DuckDBScalar,
    DuckDBStruct,
    parse_type,
    quote_identifier,
    render_type,
)
from target_ducklake.parquet_utils import (
    StagingRules,
    create_pyarrow_table,
    date_time_paths,
    nested_leaf_paths,
    staging_type,
)

if TYPE_CHECKING:
    from pathlib import Path

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

    def test_into_ducklake(self, tmp_path: Path):
        """A DuckLake table on a SQLite catalog, read under a non-UTC zone."""
        staged = self._staged_file(tmp_path)
        con = duckdb.connect()
        con.execute("SET TimeZone = 'America/New_York'")
        con.execute(
            f"ATTACH 'ducklake:sqlite:{tmp_path / 'catalog.sqlite'}' AS lake "
            f"(DATA_PATH '{tmp_path / 'data'}/', DATA_INLINING_ROW_LIMIT 0)"
        )
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
