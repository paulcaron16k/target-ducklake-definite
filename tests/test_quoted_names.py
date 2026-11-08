"""Schema, table and column names with quotes, spaces and dots (DUCKLKD-128).

Each name reaches SQL as a quoted identifier, a string literal or a bound
parameter, escaped; so do staging paths, which hold the stream name.
"""

from __future__ import annotations

import io
import json
from typing import TYPE_CHECKING, Any

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
    messages = [
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
    loader = Targetducklake(
        config={**lake.config(), "default_target_schema": schema, **config}
    )
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
