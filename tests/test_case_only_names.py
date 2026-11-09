"""Schema, table and column names that differ only in case (DUCKLKD-135).

DuckDB resolves identifiers ignoring ASCII case, but the catalog keeps each
name in the case it was created with. Every lookup of a table by name must
ignore case as DuckDB does, or a stream ``orders`` loading the table created
as ``Orders`` misses it.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from target_ducklake.collisions import TableCollisionError
from target_ducklake.target import Targetducklake
from target_ducklake.write_completed import PRODUCER
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
NATS = {"servers": "nats://127.0.0.1:1", "subject": "lake.written"}


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


def _orders_then_wider_orders(second_keys: list[str]) -> list[dict]:
    """``Orders`` loads id 1, then ``orders`` declares ``extra`` and loads id 2."""
    return [
        _schema("Orders", ["id"]),
        *_records("Orders", [{"id": 1, "v": "a"}]),
        _schema("orders", second_keys, WIDER),
        *_records("orders", [{"id": 2, "v": "b", "extra": "x"}]),
    ]


def test_a_stream_adds_its_column_to_the_table_of_a_stream_in_another_case(
    lake: Lake,
):
    """``orders`` adds ``extra`` to the table ``Orders`` created in the run."""
    _run(lake, _orders_then_wider_orders(["id"]))
    assert _rows(lake) == BOTH_ROWS


def test_streams_in_another_case_with_different_keys_are_refused(lake: Lake):
    """DUCKLKD-44 refuses them before the second stream adds its column."""
    _load(lake, "Orders", {"id": 1, "v": "a"})
    with pytest.raises(TableCollisionError, match=r"\['id'\] and \['v'\]"):
        _run(lake, _orders_then_wider_orders(["v"]))
    assert [row[0] for row in lake.query("DESCRIBE lake.s.orders")] == ["id", "v"]


def test_one_write_completed_event_for_streams_in_another_case(
    lake: Lake,
    published: list[dict],  # noqa: F811 (the fixture)
):
    """One table, one event: both streams and their rows, the table's spelling."""
    _run(lake, _orders_then_wider_orders(["id"]), nats=NATS)
    (event,) = published
    data = event["data"]
    assert (data["table"], data["rows"]) == ("Orders", 2)
    assert sorted(data["streams"]) == ["Orders", "orders"]
    assert event["id"].endswith(f"/s/Orders@{data['snapshot_id']}")


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
    """The run's commit to ``Orders`` is found for the stream ``orders``.

    The event names the table as the catalog does.
    """
    _load(lake, "Orders", {"id": 1, "v": "a"})
    _load(lake, "orders", {"id": 2, "v": "b"}, nats=NATS)
    (event,) = published
    assert (event["data"]["table"], event["data"]["rows"]) == ("Orders", 1)


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

    def test_this_runs_snapshots(self, connector: DuckLakeConnector):
        """The snapshots this run committed to it, for writeCompleted."""
        connector.tag_commits("run-135")
        connector.execute(
            "BEGIN TRANSACTION; "  # noqa: S608 (no value in the SQL)
            f"CALL {connector.catalog_name}.set_commit_message('{PRODUCER}', 'm', "
            """extra_info => '{"run_id": "run-135"}'); """
            f"INSERT INTO {connector.table_ref('s', 'Orders')} VALUES (2, 'b'); "
            "COMMIT;"
        )
        snapshots = connector.table_snapshots("S", "orders")
        assert snapshots is not None
        assert snapshots.snapshot_count == 1

    def test_the_catalogs_spelling(self, connector: DuckLakeConnector):
        """The schema and table as the catalog spells them, or None."""
        assert connector.stored_table_name("S", "orders") == ("s", "Orders")
        assert connector.stored_table_name("s", "customers") is None
