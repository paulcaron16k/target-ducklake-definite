"""Schema, table and column names that differ only in case (DUCKLKD-135).

DuckDB resolves identifiers ignoring ASCII case, but the catalog keeps each
name in the case it was created with. Every lookup of a table by name must
ignore case as DuckDB does, or a stream ``orders`` loading the table created
as ``Orders`` misses it.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from target_ducklake.target import Targetducklake
from tests.test_table_collisions import _records, _run, _schema
from tests.test_write_completed import published  # noqa: F401 (a fixture)

if TYPE_CHECKING:
    from collections.abc import Iterator

    from target_ducklake.connector import DuckLakeConnector
    from tests.catalogs import Lake

WIDER = {
    "id": {"type": "integer"},
    "v": {"type": ["null", "string"]},
    "extra": {"type": ["null", "string"]},
}
BOTH_ROWS = [(1, "a", None), (2, "b", "x")]


def _load(lake: Lake, stream: str, row: dict, **config: object) -> None:
    """Load ``row``, declaring ``extra`` only if the row has it."""
    properties = WIDER if "extra" in row else None
    _run(
        lake,
        [_schema(stream, ["id"], properties), *_records(stream, [row])],
        **config,
    )


def _rows(lake: Lake) -> list[tuple]:
    return lake.query("SELECT id, v, extra FROM lake.s.orders ORDER BY id")


def test_a_stream_adds_its_column_to_the_table_of_a_stream_in_another_case(
    lake: Lake,
):
    """``orders`` adds ``extra`` to the table ``Orders`` created in the run."""
    _run(
        lake,
        [
            _schema("Orders", ["id"]),
            *_records("Orders", [{"id": 1, "v": "a"}]),
            _schema("orders", ["id"], WIDER),
            *_records("orders", [{"id": 2, "v": "b", "extra": "x"}]),
        ],
    )
    assert _rows(lake) == BOTH_ROWS


@pytest.mark.parametrize(
    ("created_as", "loaded_as"), [("orders", "Orders"), ("Orders", "orders")]
)
def test_a_stream_adds_its_column_to_a_table_created_in_another_case(
    lake: Lake, created_as: str, loaded_as: str
):
    """A later run's stream finds the table an earlier run created."""
    _load(lake, created_as, {"id": 1, "v": "a"})
    _load(lake, loaded_as, {"id": 2, "v": "b", "extra": "x"})
    assert _rows(lake) == BOTH_ROWS
    assert lake.query(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_catalog = 'lake' AND table_schema = 's'"
    ) == [(created_as,)]


def test_a_schema_in_another_case_is_the_same_schema(lake: Lake):
    """``default_target_schema: S`` loads the schema created as ``s``."""
    _load(lake, "orders", {"id": 1, "v": "a"})
    _load(lake, "orders", {"id": 2, "v": "b", "extra": "x"}, default_target_schema="S")
    assert _rows(lake) == BOTH_ROWS


@pytest.mark.parametrize(
    ("column", "prop"), [("OrderID", "orderid"), ("orderid", "OrderID")]
)
def test_a_property_in_another_case_loads_its_column(
    lake: Lake, column: str, prop: str
):
    """The property's values go into the column, and no column is added."""
    for name, value in ((column, 1), (prop, 2)):
        _run(
            lake,
            [
                _schema("orders", [name], {name: {"type": "integer"}}),
                *_records("orders", [{name: value}]),
            ],
        )
    assert lake.query("SELECT * FROM lake.s.orders ORDER BY 1") == [(1,), (2,)]
    assert [row[0] for row in lake.query("DESCRIBE lake.s.orders")] == [column]


def test_write_completed_counts_a_table_created_in_another_case(
    lake: Lake,
    published: list[dict],  # noqa: F811 (the fixture)
):
    """The run's commit to ``Orders`` is found for the stream ``orders``."""
    _load(lake, "Orders", {"id": 1, "v": "a"})
    _load(
        lake,
        "orders",
        {"id": 2, "v": "b"},
        nats={"servers": "nats://127.0.0.1:1", "subject": "lake.written"},
    )
    (event,) = published
    assert (event["data"]["table"], event["data"]["rows"]) == ("orders", 1)


class TestCatalogLookups:
    """Each read of a table's metadata finds the table ``Orders`` as ``orders``."""

    @pytest.fixture
    def connector(self, lake: Lake) -> Iterator[DuckLakeConnector]:
        """A connector on a lake holding ``Orders``, partitioned, a row inlined."""
        lake.query(
            "CREATE SCHEMA lake.s; "
            "CREATE TABLE lake.s.Orders (id BIGINT, v VARCHAR); "
            "ALTER TABLE lake.s.Orders SET PARTITIONED BY (id); "
            "CALL lake.set_option('data_inlining_row_limit', 10, "
            "schema => 's', table_name => 'Orders'); "
            "INSERT INTO lake.s.Orders VALUES (1, 'a')"
        )
        made = Targetducklake(config=lake.config()).target_connector
        try:
            yield made
        finally:
            made.close()

    def test_partition_keys(self, connector: DuckLakeConnector):
        """Its partition keys, so they aren't set again each run."""
        layout = connector.get_table_layout("S", "orders")
        assert [key.column for key in layout.partition or ()] == ["id"]

    def test_inlining_limit(self, connector: DuckLakeConnector):
        """Its own data_inlining_row_limit, not the schema's or the global one."""
        assert connector.table_inlining_limit("S", "orders") == 10  # noqa: PLR2004

    def test_inlined_rows(self, connector: DuckLakeConnector):
        """Its inlined rows, so turning inlining off flushes them."""
        assert connector.inlined_row_count("S", "orders") == 1
