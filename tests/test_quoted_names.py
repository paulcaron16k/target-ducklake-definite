"""Schema, table and column names with quotes, spaces and dots (DUCKLKD-128).

Each name reaches SQL as a quoted identifier, a string literal or a bound
parameter, escaped; so do staging paths, which hold the stream name.
"""

from __future__ import annotations

import io
import json
import re
from typing import TYPE_CHECKING, Any

import duckdb
import pytest

from target_ducklake.connector import DuckLakeConnector
from target_ducklake.layout import PartitionKey, SortKey, TableLayout
from target_ducklake.target import Targetducklake

if TYPE_CHECKING:
    from tests.catalogs import Lake

# A single quote ends a string literal; a double quote ends an identifier.
STREAM = "it's"
SCHEMA = 'it\'s "s"'
COLUMN = 'a "b" it\'s'
# Unquoted, the space and the dot would split the name.
DOTTED_STREAM = 'say "hi". now'

# (stream, value column, target schema): the issue's stream, then every
# kind of name quoted at once.
NAMES = [
    pytest.param(STREAM, "v", "s", id="single-quote-stream"),
    pytest.param(DOTTED_STREAM, COLUMN, SCHEMA, id="all-names-quoted"),
]


def _ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _load(  # noqa: PLR0913 (the stream, then its names and the config)
    lake: Lake,
    stream: str,
    records: list[dict],
    *,
    column: str = "v",
    schema: str = "s",
    version: int = 1,
    extra: list[dict] | None = None,
    **config: object,
) -> None:
    """One run of the target over the stream's records, keyed by ``id``."""
    properties: dict[str, Any] = {
        "id": {"type": "integer"},
        column: {"type": ["null", "string"]},
        "_sdc_deleted_at": {"type": ["null", "string"], "format": "date-time"},
    }
    messages: list[dict] = [
        {
            "type": "SCHEMA",
            "stream": stream,
            "schema": {"type": "object", "properties": properties},
            "key_properties": ["id"],
        },
        *(
            {"type": "RECORD", "stream": stream, "record": record, "version": version}
            for record in records
        ),
        *(extra or []),
    ]
    _listen(lake, messages, default_target_schema=schema, **config)


def _listen(lake: Lake, messages: list[dict], **config: object) -> None:
    """One run of the target over the messages."""
    loader = Targetducklake(config={**lake.config(), **config})
    try:
        loader.listen(
            file_input=io.StringIO("".join(json.dumps(m) + "\n" for m in messages))
        )
    finally:
        loader.target_connector.close()


def _rows(lake: Lake, stream: str, column: str, schema: str) -> list[tuple]:
    table = f"lake.{_ident(schema)}.{_ident(stream)}"
    return lake.query(f"SELECT id, {_ident(column)} FROM {table} ORDER BY ALL")  # noqa: S608


def _connector(lake: Lake) -> DuckLakeConnector:
    return DuckLakeConnector(lake.config())


INLINING_LIMIT = 10
PROPERTIES = {"id": {"type": "integer"}}
FIRST = [{"id": 1, "v": "a"}]
SECOND = [{"id": 1, "v": "b"}, {"id": 2, "v": "c"}]
EXPECTED = {
    "append": [(1, "a"), (1, "b"), (2, "c")],
    "merge": [(1, "b"), (2, "c")],
    "overwrite": [(1, "b"), (2, "c")],
}


def _named(records: list[dict], column: str) -> list[dict]:
    """The records with their value under the column's name."""
    return [{"id": r["id"], column: r["v"]} for r in records]


class TestThroughListen:
    """Loads through ``listen`` (SQLite, and Postgres when set)."""

    @pytest.mark.parametrize("load_method", ["append", "merge", "overwrite"])
    @pytest.mark.parametrize(("stream", "column", "schema"), NAMES)
    def test_each_load_method(
        self, lake: Lake, load_method: str, stream: str, column: str, schema: str
    ):
        """A second run finds the table, its columns, and loads into it."""
        for records in (FIRST, SECOND):
            _load(
                lake,
                stream,
                _named(records, column),
                column=column,
                schema=schema,
                load_method=load_method,
            )
        assert _rows(lake, stream, column, schema) == EXPECTED[load_method]

    @pytest.mark.parametrize(("stream", "column", "schema"), NAMES)
    def test_a_new_column_is_added(
        self, lake: Lake, stream: str, column: str, schema: str
    ):
        """The column the second run declares is added by its quoted name."""
        _load(lake, stream, FIRST, schema=schema)
        _load(lake, stream, _named(SECOND, column), column=column, schema=schema)
        assert _rows(lake, stream, column, schema) == [(1, "b"), (2, "c")]

    @pytest.mark.parametrize("hard_delete", [False, True], ids=["soft", "hard"])
    def test_activate_version_and_deleted_rows(self, lake: Lake, *, hard_delete: bool):
        """ACTIVATE_VERSION retires, and ``_sdc_deleted_at`` deletes, by name."""
        activate = {"type": "ACTIVATE_VERSION", "stream": DOTTED_STREAM, "version": 2}
        deleted = {"id": 3, COLUMN: "d", "_sdc_deleted_at": "2024-01-01T00:00:00Z"}
        for version, records, extra in (
            (1, _named(FIRST, COLUMN), []),
            (2, [{"id": 2, COLUMN: "c"}, deleted], [activate]),
        ):
            _load(
                lake,
                DOTTED_STREAM,
                records,
                column=COLUMN,
                schema=SCHEMA,
                version=version,
                extra=extra,
                add_record_metadata=True,
                hard_delete=hard_delete,
                delete_rows_with_deleted_at=True,
            )
        table = f"lake.{_ident(SCHEMA)}.{_ident(DOTTED_STREAM)}"
        live = lake.query(
            f"SELECT id FROM {table} WHERE _sdc_deleted_at IS NULL"  # noqa: S608
        )
        # 1 is retired with version 1; 3 was deleted at the source.
        assert live == [(2,)]

    def test_partition_and_sort_keys(self, lake: Lake):
        """The layout is set, read back, and left alone when unchanged."""
        partition = {DOTTED_STREAM: {"partition": [{"column": COLUMN}]}}
        for records in (FIRST, SECOND):
            _load(
                lake,
                DOTTED_STREAM,
                _named(records, COLUMN),
                column=COLUMN,
                schema=SCHEMA,
                stream_overrides=partition,
            )
        # sort_order's text form can't name a column with a double quote, so
        # the sort key is applied directly.
        sort = TableLayout(sort=(SortKey(COLUMN, "DESC", "NULLS_FIRST"),))
        snapshots = "SELECT count(*) FROM lake.snapshots()"
        connector = _connector(lake)
        try:
            connector.apply_table_layout(SCHEMA, DOTTED_STREAM, sort)
            before = lake.query(snapshots)
            connector.apply_table_layout(SCHEMA, DOTTED_STREAM, sort)
            assert lake.query(snapshots) == before  # unchanged: not re-applied
            table_layout = connector.get_table_layout(SCHEMA, DOTTED_STREAM)
        finally:
            connector.close()
        assert table_layout.partition == (PartitionKey(COLUMN, "identity"),)
        assert table_layout.sort == sort.sort

    def test_inlined_rows_and_their_flush(self, lake: Lake):
        """The table's inlining option and inlined rows are found by name."""
        _load(
            lake,
            STREAM,
            FIRST,
            schema=SCHEMA,
            data_inlining_row_limit=INLINING_LIMIT,
        )
        connector = _connector(lake)
        try:
            assert connector.table_inlining_limit(SCHEMA, STREAM) == INLINING_LIMIT
            assert connector.inlined_row_count(SCHEMA, STREAM) == 1
        finally:
            connector.close()
        _load(
            lake,
            STREAM,
            SECOND,
            schema=SCHEMA,
            data_inlining_row_limit=INLINING_LIMIT,
            inlined_data_flush_rows=1,
            load_method="merge",
        )
        connector = _connector(lake)
        try:
            assert connector.inlined_row_count(SCHEMA, STREAM) == 0
        finally:
            connector.close()
        assert _rows(lake, STREAM, "v", SCHEMA) == [(1, "b"), (2, "c")]

    @pytest.mark.parametrize(
        ("load_method", "expected"),
        [
            ("merge", [(1, "b", None), (2, "c", "y")]),
            ("append", [(1, "a", None), (1, "b", None), (2, "c", "y")]),
        ],
    )
    def test_a_table_created_unquoted_before(
        self, lake: Lake, load_method: str, expected: list[tuple]
    ):
        """A mixed-case table the old, unquoted SQL made is found and loaded.

        The table, its schema and its columns are named as before, unquoted;
        DuckLake resolves the quoted names case-insensitively, as unquoted
        ones, so no second table or column is made.
        """
        con = duckdb.connect()
        con.execute(lake.attach_sql())
        con.execute("CREATE SCHEMA lake.Raw")
        con.execute("CREATE TABLE lake.Raw.Orders (OrderID BIGINT, Name VARCHAR)")
        con.execute("INSERT INTO lake.Raw.Orders VALUES (1, 'a')")
        con.close()
        properties: dict[str, Any] = {
            "OrderID": {"type": "integer"},
            "Name": {"type": ["null", "string"]},
        }
        runs = [
            [{"OrderID": 1, "Name": "b"}],
            [{"OrderID": 2, "Name": "c", "NewCol": "y"}],
        ]
        for records in runs:
            if "NewCol" in records[0]:
                properties["NewCol"] = {"type": ["null", "string"]}
            schema = {"type": "object", "properties": properties}
            _listen(
                lake,
                [
                    {
                        "type": "SCHEMA",
                        "stream": "Raw-Orders",
                        "schema": schema,
                        "key_properties": ["OrderID"],
                    },
                    *(
                        {"type": "RECORD", "stream": "Raw-Orders", "record": r}
                        for r in records
                    ),
                ],
                default_target_schema=None,
                load_method=load_method,
            )
        assert (
            lake.query("SELECT OrderID, Name, NewCol FROM lake.Raw.Orders ORDER BY ALL")
            == expected
        )
        assert lake.query(
            "SELECT table_schema, table_name FROM information_schema.tables "
            "WHERE table_catalog = 'lake'"
        ) == [("Raw", "Orders")]
        assert [
            name
            for (name,) in lake.query(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_catalog = 'lake' ORDER BY ordinal_position"
            )
        ] == ["OrderID", "Name", "NewCol"]

    def test_a_slash_in_the_stream_name(self, lake: Lake):
        """The staging path replaces it; the table keeps it."""
        _load(lake, "a/b", FIRST)
        assert _rows(lake, "a/b", "v", "s") == [(1, "a")]

    @pytest.mark.parametrize(
        ("stream", "schema", "empty"),
        [
            ("orders-", "s", "table"),
            ("x'; DROP TABLE t; --", "s", "table"),
            ("-orders", None, "schema"),
            ("db--orders", None, "schema"),
        ],
    )
    def test_an_empty_schema_or_table_segment(
        self, lake: Lake, stream: str, schema: str | None, empty: str
    ):
        """'-' splits the stream name; an empty part is refused by name."""
        with pytest.raises(
            ValueError,
            match=rf"^Stream {re.escape(repr(stream))} gives an empty {empty} name",
        ):
            _listen(
                lake,
                [
                    {
                        "type": "SCHEMA",
                        "stream": stream,
                        "schema": {"type": "object", "properties": PROPERTIES},
                        "key_properties": ["id"],
                    },
                    {"type": "RECORD", "stream": stream, "record": {"id": 1}},
                ],
                default_target_schema=schema,
            )


class TestSql:
    """The SQL the connector writes for such names."""

    def test_table_ref_quotes_the_schema_and_table(self):
        """Each name is one quoted identifier, a double quote doubled."""
        connector = DuckLakeConnector(
            {"catalog_type": "duckdb", "catalog_url": "unused", "data_path": "/d/"}
        )
        assert connector.table_ref(SCHEMA, DOTTED_STREAM) == (
            'ducklake_catalog."it\'s ""s"""."say ""hi"". now"'
        )

    def test_attach_escapes_its_string_literals(self):
        """A quote in the metadata schema or the data path stays in the literal."""
        connector = DuckLakeConnector(
            {
                "catalog_type": "postgres",
                "catalog_url": "dbname=lake",
                "data_path": "/data/it's/",
                "meta_schema": "it's",
                "storage_type": "local",
            }
        )
        assert connector._build_attach_statement() == (  # noqa: SLF001
            "ATTACH 'ducklake:postgres:dbname=lake' AS ducklake_catalog "
            "(DATA_PATH '/data/it''s/', DATA_INLINING_ROW_LIMIT 0, "
            "META_SCHEMA 'it''s');"
        )

    def test_attach_takes_a_libpq_quoted_value_literally(self):
        """A conninfo value quoted for libpq reaches libpq as written.

        Before DUCKLKD-128 the value was pasted into the literal unescaped,
        so a quoted password had to be written with its quotes doubled.
        """
        connector = DuckLakeConnector(
            {
                "catalog_type": "postgres",
                "catalog_url": "host=h user=u password='my pw' dbname=d",
                "data_path": "/data/",
                "storage_type": "local",
            }
        )
        statement = connector._build_attach_statement()  # noqa: SLF001
        assert statement == (
            "ATTACH 'ducklake:postgres:host=h user=u password=''my pw'' dbname=d' "
            "AS ducklake_catalog (DATA_PATH '/data/', DATA_INLINING_ROW_LIMIT 0);"
        )
        location = statement.removeprefix("ATTACH ").split(" AS ")[0]
        assert duckdb.connect().execute(f"SELECT {location}").fetchone() == (
            "ducklake:postgres:host=h user=u password='my pw' dbname=d",
        )

    @pytest.mark.parametrize("storage_type", ["S3", "GCS"])
    def test_a_quote_in_a_secret_stays_in_it(self, storage_type: str):
        """CREATE SECRET writes each value as one escaped literal."""
        connector = DuckLakeConnector(
            {
                "catalog_type": "duckdb",
                "catalog_url": "unused",
                "data_path": "s3://bucket/",
                "storage_type": storage_type,
                "public_key": "key'id",
                "secret_key": "se'cret",
                "region": "us-east-1",
            }
        )
        script = connector._build_startup_script()  # noqa: SLF001
        assert "KEY_ID 'key''id'" in script
        assert "SECRET 'se''cret'" in script
